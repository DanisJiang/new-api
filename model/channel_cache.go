package model

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
)

var group2model2channels map[string]map[string][]int // enabled channel
var channelsIDM map[int]*Channel                     // all channels include disabled
var channelSyncLock sync.RWMutex

// emptyAnswerCache tracks (request_hash -> channel_ids) for requests where a
// thinking model returned reasoning but no answer content. When the engine
// retries with the same request, we exclude those channels from selection.
var emptyAnswerCache sync.Map // map[string]*emptyAnswerRecord

// duplicateResponseCache tracks token counts per (requestHash:channelId) to
// detect channels that repeatedly produce the same truncated output. When a
// second response with identical input+output token counts arrives within 10
// minutes on the same channel, the channel is excluded for future requests.
var duplicateResponseCache sync.Map // map[string]*tokenResponseRecord

type emptyAnswerEntry struct {
	Expiry    time.Time
	RequestID string
}

type emptyAnswerRecord struct {
	mu       sync.Mutex
	channels map[int]*emptyAnswerEntry // channelID -> entry
}

type tokenResponseRecord struct {
	InputTokens  int
	OutputTokens int
	Timestamp    time.Time
	RequestID    string
}

// RecordEmptyAnswer marks a channel as having produced an empty answer for the
// given request hash. The entry expires after ttl. sourceRequestID is the
// request that triggered the recording, shown in the UI for debugging.
func RecordEmptyAnswer(requestHash string, channelID int, ttl time.Duration, sourceRequestID string) {
	if requestHash == "" {
		return
	}
	actual, _ := emptyAnswerCache.LoadOrStore(requestHash, &emptyAnswerRecord{
		channels: make(map[int]*emptyAnswerEntry),
	})
	record := actual.(*emptyAnswerRecord)
	record.mu.Lock()
	record.channels[channelID] = &emptyAnswerEntry{
		Expiry:    time.Now().Add(ttl),
		RequestID: sourceRequestID,
	}
	record.mu.Unlock()
}

// GetExcludedChannels returns channel IDs that should be skipped for this
// request hash because they previously produced empty answers. Expired
// entries are cleaned up lazily.
func GetExcludedChannels(requestHash string) []int {
	if requestHash == "" {
		return nil
	}
	val, ok := emptyAnswerCache.Load(requestHash)
	if !ok {
		return nil
	}
	record := val.(*emptyAnswerRecord)
	record.mu.Lock()
	defer record.mu.Unlock()

	now := time.Now()
	var excluded []int
	for chID, entry := range record.channels {
		if now.Before(entry.Expiry) {
			excluded = append(excluded, chID)
		} else {
			delete(record.channels, chID)
		}
	}
	// Clean up empty records
	if len(record.channels) == 0 {
		emptyAnswerCache.Delete(requestHash)
	}
	return excluded
}

// GetExcludedChannelDetails returns a map of channelID -> sourceRequestID for
// channels that should be excluded. Used for UI display.
func GetExcludedChannelDetails(requestHash string) map[int]string {
	if requestHash == "" {
		return nil
	}
	val, ok := emptyAnswerCache.Load(requestHash)
	if !ok {
		return nil
	}
	record := val.(*emptyAnswerRecord)
	record.mu.Lock()
	defer record.mu.Unlock()

	now := time.Now()
	details := make(map[int]string)
	for chID, entry := range record.channels {
		if now.Before(entry.Expiry) {
			details[chID] = entry.RequestID
		}
	}
	if len(details) == 0 {
		return nil
	}
	return details
}

// CheckAndRecordDuplicateResponse checks whether the current response has the
// same (inputTokens, outputTokens) as a previous response from the same channel
// for the same request hash within the last 10 minutes. If so, it records the
// channel exclusion (reusing RecordEmptyAnswer) and returns true.
func CheckAndRecordDuplicateResponse(requestHash string, channelID int, inputTokens, outputTokens int, ttl time.Duration, sourceRequestID string) bool {
	if requestHash == "" || outputTokens == 0 {
		return false
	}

	key := fmt.Sprintf("%s:%d", requestHash, channelID)
	now := time.Now()

	if prev, ok := duplicateResponseCache.Load(key); ok {
		record := prev.(*tokenResponseRecord)
		if now.Sub(record.Timestamp) <= 10*time.Minute &&
			record.InputTokens == inputTokens &&
			record.OutputTokens == outputTokens {
			// Duplicate detected — exclude this channel
			RecordEmptyAnswer(requestHash, channelID, ttl, sourceRequestID)
			return true
		}
	}

	// Store/update current response record
	duplicateResponseCache.Store(key, &tokenResponseRecord{
		InputTokens:  inputTokens,
		OutputTokens: outputTokens,
		Timestamp:    now,
		RequestID:    sourceRequestID,
	})

	return false
}

func InitChannelCache() {
	if !common.MemoryCacheEnabled {
		return
	}
	newChannelId2channel := make(map[int]*Channel)
	var channels []*Channel
	DB.Find(&channels)
	for _, channel := range channels {
		newChannelId2channel[channel.Id] = channel
	}
	var abilities []*Ability
	DB.Find(&abilities)
	groups := make(map[string]bool)
	for _, ability := range abilities {
		groups[ability.Group] = true
	}
	newGroup2model2channels := make(map[string]map[string][]int)
	for group := range groups {
		newGroup2model2channels[group] = make(map[string][]int)
	}
	for _, channel := range channels {
		if channel.Status != common.ChannelStatusEnabled {
			continue // skip disabled channels
		}
		groups := strings.Split(channel.Group, ",")
		for _, group := range groups {
			models := strings.Split(channel.Models, ",")
			for _, model := range models {
				if _, ok := newGroup2model2channels[group][model]; !ok {
					newGroup2model2channels[group][model] = make([]int, 0)
				}
				newGroup2model2channels[group][model] = append(newGroup2model2channels[group][model], channel.Id)
			}
		}
	}

	// sort by priority
	for group, model2channels := range newGroup2model2channels {
		for model, channels := range model2channels {
			sort.Slice(channels, func(i, j int) bool {
				return newChannelId2channel[channels[i]].GetPriority() > newChannelId2channel[channels[j]].GetPriority()
			})
			newGroup2model2channels[group][model] = channels
		}
	}

	channelSyncLock.Lock()
	group2model2channels = newGroup2model2channels
	//channelsIDM = newChannelId2channel
	for i, channel := range newChannelId2channel {
		if channel.ChannelInfo.IsMultiKey {
			channel.Keys = channel.GetKeys()
			if channel.ChannelInfo.MultiKeyMode == constant.MultiKeyModePolling {
				if oldChannel, ok := channelsIDM[i]; ok {
					// 存在旧的渠道，如果是多key且轮询，保留轮询索引信息
					if oldChannel.ChannelInfo.IsMultiKey && oldChannel.ChannelInfo.MultiKeyMode == constant.MultiKeyModePolling {
						channel.ChannelInfo.MultiKeyPollingIndex = oldChannel.ChannelInfo.MultiKeyPollingIndex
					}
				}
			}
		}
	}
	channelsIDM = newChannelId2channel
	channelSyncLock.Unlock()
	common.SysLog("channels synced from database")
}

func SyncChannelCache(frequency int) {
	for {
		time.Sleep(time.Duration(frequency) * time.Second)
		common.SysLog("syncing channels from database")
		InitChannelCache()
	}
}

func isChannelExcluded(channelID int, excluded []int) bool {
	for _, id := range excluded {
		if id == channelID {
			return true
		}
	}
	return false
}

func GetRandomSatisfiedChannel(group string, model string, retry int, excludedChannels []int) (*Channel, error) {
	// if memory cache is disabled, get channel directly from database
	if !common.MemoryCacheEnabled {
		ch, err := GetChannel(group, model, retry, excludedChannels)
		if err != nil {
			return nil, err
		}
		// If exclusion made all channels unavailable, fall back to no exclusion
		if ch == nil && len(excludedChannels) > 0 {
			return GetChannel(group, model, retry, nil)
		}
		return ch, err
	}

	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	// First, try to find channels with the exact model name.
	channels := group2model2channels[group][model]

	// If no channels found, try to find channels with the normalized model name.
	if len(channels) == 0 {
		normalizedModel := ratio_setting.FormatMatchingModelName(model)
		channels = group2model2channels[group][normalizedModel]
	}

	if len(channels) == 0 {
		return nil, nil
	}

	if len(channels) == 1 {
		// If the only channel is excluded, ignore the exclusion (fall back)
		if channel, ok := channelsIDM[channels[0]]; ok {
			return channel, nil
		}
		return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channels[0])
	}

	uniquePriorities := make(map[int]bool)
	for _, channelId := range channels {
		if isChannelExcluded(channelId, excludedChannels) {
			continue
		}
		if channel, ok := channelsIDM[channelId]; ok {
			uniquePriorities[int(channel.GetPriority())] = true
		} else {
			return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channelId)
		}
	}
	// If all channels are excluded, fall back to no exclusion
	if len(uniquePriorities) == 0 && len(excludedChannels) > 0 {
		return GetRandomSatisfiedChannel(group, model, retry, nil)
	}

	var sortedUniquePriorities []int
	for priority := range uniquePriorities {
		sortedUniquePriorities = append(sortedUniquePriorities, priority)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sortedUniquePriorities)))

	if retry >= len(uniquePriorities) {
		retry = len(uniquePriorities) - 1
	}
	targetPriority := int64(sortedUniquePriorities[retry])

	// get the priority for the given retry number
	var sumWeight = 0
	var targetChannels []*Channel
	for _, channelId := range channels {
		if isChannelExcluded(channelId, excludedChannels) {
			continue
		}
		if channel, ok := channelsIDM[channelId]; ok {
			if channel.GetPriority() == targetPriority {
				sumWeight += channel.GetWeight()
				targetChannels = append(targetChannels, channel)
			}
		} else {
			return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channelId)
		}
	}

	if len(targetChannels) == 0 {
		return nil, errors.New(fmt.Sprintf("no channel found, group: %s, model: %s, priority: %d", group, model, targetPriority))
	}

	// smoothing factor and adjustment
	smoothingFactor := 1
	smoothingAdjustment := 0

	if sumWeight == 0 {
		// when all channels have weight 0, set sumWeight to the number of channels and set smoothing adjustment to 100
		// each channel's effective weight = 100
		sumWeight = len(targetChannels) * 100
		smoothingAdjustment = 100
	} else if sumWeight/len(targetChannels) < 10 {
		// when the average weight is less than 10, set smoothing factor to 100
		smoothingFactor = 100
	}

	// Calculate the total weight of all channels up to endIdx
	totalWeight := sumWeight * smoothingFactor

	// Generate a random value in the range [0, totalWeight)
	randomWeight := rand.Intn(totalWeight)

	// Find a channel based on its weight
	for _, channel := range targetChannels {
		randomWeight -= channel.GetWeight()*smoothingFactor + smoothingAdjustment
		if randomWeight < 0 {
			return channel, nil
		}
	}
	// return null if no channel is not found
	return nil, errors.New("channel not found")
}

func CacheGetChannel(id int) (*Channel, error) {
	if !common.MemoryCacheEnabled {
		return GetChannelById(id, true)
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	c, ok := channelsIDM[id]
	if !ok {
		return nil, fmt.Errorf("渠道# %d，已不存在", id)
	}
	return c, nil
}

func CacheGetChannelInfo(id int) (*ChannelInfo, error) {
	if !common.MemoryCacheEnabled {
		channel, err := GetChannelById(id, true)
		if err != nil {
			return nil, err
		}
		return &channel.ChannelInfo, nil
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	c, ok := channelsIDM[id]
	if !ok {
		return nil, fmt.Errorf("渠道# %d，已不存在", id)
	}
	return &c.ChannelInfo, nil
}

func CacheUpdateChannelStatus(id int, status int) {
	if !common.MemoryCacheEnabled {
		return
	}
	channelSyncLock.Lock()
	defer channelSyncLock.Unlock()
	if channel, ok := channelsIDM[id]; ok {
		channel.Status = status
	}
	if status != common.ChannelStatusEnabled {
		// delete the channel from group2model2channels
		for group, model2channels := range group2model2channels {
			for model, channels := range model2channels {
				for i, channelId := range channels {
					if channelId == id {
						// remove the channel from the slice
						group2model2channels[group][model] = append(channels[:i], channels[i+1:]...)
						break
					}
				}
			}
		}
	}
}

func CacheUpdateChannel(channel *Channel) {
	if !common.MemoryCacheEnabled {
		return
	}
	channelSyncLock.Lock()
	defer channelSyncLock.Unlock()
	if channel == nil {
		return
	}

	println("CacheUpdateChannel:", channel.Id, channel.Name, channel.Status, channel.ChannelInfo.MultiKeyPollingIndex)

	println("before:", channelsIDM[channel.Id].ChannelInfo.MultiKeyPollingIndex)
	channelsIDM[channel.Id] = channel
	println("after :", channelsIDM[channel.Id].ChannelInfo.MultiKeyPollingIndex)
}

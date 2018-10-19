package model

import (
	"container/heap"
	"sync"
)

// SimpleRollingWindows is an implementation of `RollingWindows`
type SimpleRollingWindows struct {
	m       sync.RWMutex
	width   int
	windows []GetKeyCounter
	scorer  KeyScorer
	// the widnows slice looks like this:
	// [readFrom ... readTo, current]
	// <--     width    -->
	// so its actual length is always `width + 1`
	// when it rolls, every mark shifts to the right by 1
	// prev `readFrom` becomes the new `current`
	current   int
	readFrom  int
	readTo    int
	topN      int
	threshold uint64
}

// NewSimpleRollingWindows initialize a `SimpleRollingWindows` struct with the writable `current` and empty `[readFrom, readTo]` windows
func NewSimpleRollingWindows(scorer KeyScorer, keyCounterGenerator func() GetKeyCounter, rollingWidth int, topN int, threshold uint64) *SimpleRollingWindows {

	rollingWindows := make([]GetKeyCounter, rollingWidth+1)
	for w := 0; w <= rollingWidth; w++ {
		rollingWindows[w] = &EmptyGetKeyCounter{}
	}
	// initialize the `current` window as a writable window
	rollingWindows[rollingWidth] = keyCounterGenerator()

	return &SimpleRollingWindows{
		m:         sync.RWMutex{},
		width:     rollingWidth,
		windows:   rollingWindows,
		scorer:    scorer,
		current:   rollingWidth,
		readFrom:  0,
		readTo:    rollingWidth - 1,
		topN:      topN,
		threshold: threshold,
	}
}

func (simpleRollingWindows *SimpleRollingWindows) last() GetKeyCounter {
	simpleRollingWindows.m.RLock()
	defer simpleRollingWindows.m.RUnlock()
	return simpleRollingWindows.windows[simpleRollingWindows.current]
}

// Increment always finds the `last()` window and delegates there
func (simpleRollingWindows *SimpleRollingWindows) Increment(key string, delta uint64) {
	simpleRollingWindows.last().Increment(key, delta)
}

// Scorer is a getter for `KeyScorer`
func (simpleRollingWindows *SimpleRollingWindows) Scorer() KeyScorer {
	return simpleRollingWindows.scorer
}

func topN(scorer KeyScorer, all map[string]uint64, n int, threshold uint64) map[string]uint64 {

	hotKeyEntries := HotKeyEntries(make([]*HotKeyEntry, 0, len(all)))
	for k, c := range all {
		if c >= threshold {
			score := scorer.GetScore(k)
			all[k] = c * score
			hotKeyEntries = append(hotKeyEntries, &HotKeyEntry{k, c * score})
		} else {
			delete(all, k)
		}
	}

	if len(all) <= n {
		return all
	}

	// the total complexity is O(m) + O(n*log(m)), and m = len(all), as n is likely to be a small constant, therefore O(m)
	heap.Init(&hotKeyEntries)
	tops := make(map[string]uint64, n)
	for t := 0; t < n; t++ {
		pop := heap.Pop(&hotKeyEntries)
		if hotKeyEntry, ok := pop.(*HotKeyEntry); ok {
			tops[hotKeyEntry.Key] = hotKeyEntry.Score
		}
	}
	return tops
}

// Roll shifts the windows, and create a new write window
func (simpleRollingWindows *SimpleRollingWindows) Roll() map[string]uint64 {
	simpleRollingWindows.m.Lock()
	defer simpleRollingWindows.m.Unlock()
	// overwrite the `readFrom` with a new `current` window
	simpleRollingWindows.windows[simpleRollingWindows.readFrom] = NewBucketGetKeyCounter(32)
	// gather all counts from all keys in the range [`readFrom` + 1, `readTo`], inclusively
	aggregate := map[string]uint64{}
	width := simpleRollingWindows.width + 1
	for s := (simpleRollingWindows.readFrom + 1) % width; s != simpleRollingWindows.readFrom; s = (s + 1) % width {
		for k, c := range simpleRollingWindows.windows[s].Snapshot() {
			aggregate[k] += c
		}
	}
	// shift `readFrom, readTo, current` to the right by exactly 1 position
	simpleRollingWindows.readTo = simpleRollingWindows.current
	simpleRollingWindows.current = simpleRollingWindows.readFrom
	simpleRollingWindows.readFrom = (simpleRollingWindows.readFrom + 1) % width
	// combine with the score and find the `topN`, in random order
	return topN(simpleRollingWindows.Scorer(), aggregate, simpleRollingWindows.topN, simpleRollingWindows.threshold)
}

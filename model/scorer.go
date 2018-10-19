package model

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

	"github.com/dghubble/trie"
)

type scoreEntry struct {
	bytes   uint64
	exptime int64
}

type SimpleKeyScorer struct {
	m        sync.RWMutex
	minBytes uint64
	trie     *trie.RuneTrie
}

func (simpleKeyScorer *SimpleKeyScorer) SetScore(key string, bytes uint64, exptime int64) {
	simpleKeyScorer.m.Lock()
	defer simpleKeyScorer.m.Unlock()

	simpleKeyScorer.trie.Put(key, &scoreEntry{bytes, exptime})
}

func (simpleKeyScorer *SimpleKeyScorer) DelScore(key ...string) {
	simpleKeyScorer.m.Lock()
	defer simpleKeyScorer.m.Unlock()

	for _, k := range key {
		simpleKeyScorer.trie.Delete(k)
	}
}

func (simpleKeyScorer *SimpleKeyScorer) GetScore(key string) uint64 {
	simpleKeyScorer.m.RLock()
	defer simpleKeyScorer.m.RUnlock()

	if entry := simpleKeyScorer.trie.Get(key); entry != nil {
		if stored, ok := entry.(*scoreEntry); ok && (stored.exptime == 0 || stored.exptime > time.Now().Unix()) {
			return stored.bytes
		}
	}
	return simpleKeyScorer.minBytes
}

func (simpleKeyScorer *SimpleKeyScorer) sweep() []string {
	simpleKeyScorer.m.RLock()
	defer simpleKeyScorer.m.RUnlock()

	expired := make([]string, 0, 1024)
	now := time.Now().Unix()
	simpleKeyScorer.trie.Walk(func(key string, val interface{}) error {
		if entry, ok := val.(*scoreEntry); ok {
			if entry.exptime > 0 && entry.exptime <= now {
				expired = append(expired, key)
			}
		}
		return nil
	})

	return expired
}

var randomDelay = rand.New(rand.NewSource(0))

func NewSimpleKeyScorer(minBytes uint64, sweepInterval time.Duration) *SimpleKeyScorer {
	scorer := &SimpleKeyScorer{
		m:        sync.RWMutex{},
		minBytes: minBytes,
		trie:     trie.NewRuneTrie(),
	}
	go func() {
		// ask the sweeper to scatter at different time
		time.Sleep(time.Duration(randomDelay.Int63n(int64(sweepInterval))))
		ticker := time.NewTicker(sweepInterval)
		for range ticker.C {
			expired := scorer.sweep()
			scorer.DelScore(expired...)
		}
	}()

	return scorer
}

type BucketKeyScorer struct {
	minBytes  uint64
	bucketing *Bucketing
	scorers   map[int]*SimpleKeyScorer
}

func (bucketKeyScorer *BucketKeyScorer) SetScore(key string, bytes uint64, exptime int64) {
	if scorer, ok := bucketKeyScorer.bucketing.Pick([]byte(key)).(KeyScorer); ok {
		scorer.SetScore(key, bytes, exptime)
	}
}

func (bucketKeyScorer *BucketKeyScorer) DelScore(key ...string) {

	groups := map[KeyScorer][]string{}
	for _, k := range key {
		scorer := bucketKeyScorer.bucketing.Pick([]byte(k)).(KeyScorer)
		groups[scorer] = append(groups[scorer], k)
	}

	for scorer, keys := range groups {
		scorer.DelScore(keys...)
	}
}

func (bucketKeyScorer *BucketKeyScorer) GetScore(key string) uint64 {

	if scorer, ok := bucketKeyScorer.bucketing.Pick([]byte(key)).(KeyScorer); ok {
		return scorer.GetScore(key)
	}

	return bucketKeyScorer.minBytes
}

func NewBucketKeyScorer(buckets int, minBytes uint64, sweepInterval time.Duration) *BucketKeyScorer {

	scorer := &BucketKeyScorer{
		minBytes: minBytes,
		bucketing: NewBucketing(func() interface{} {
			return NewSimpleKeyScorer(minBytes, sweepInterval)
		}, fnv.New32a, uint32(buckets)),
		scorers: make(map[int]*SimpleKeyScorer, buckets),
	}

	return scorer
}

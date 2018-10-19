package model

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// EmptyGetKeyCounter noops on `increment`, and gives an empty `snapshot` everytime
type EmptyGetKeyCounter struct {
}

// Increment is a noop
func (emptyGetKeyCounter *EmptyGetKeyCounter) Increment(key string, delta uint64) {

}

// Snapshot always gets an empty map
func (emptyGetKeyCounter *EmptyGetKeyCounter) Snapshot() map[string]uint64 {
	return map[string]uint64{}
}

// SimpleGetKeyCounter has a simple `counts` to track all keys' occurances
type SimpleGetKeyCounter struct {
	m      sync.Mutex
	counts map[string]uint64
}

// Increment adds `delta` count to the given `key`
func (simpleGetKeyCounter *SimpleGetKeyCounter) Increment(key string, delta uint64) {
	simpleGetKeyCounter.m.Lock()
	defer simpleGetKeyCounter.m.Unlock()
	simpleGetKeyCounter.counts[key] += delta
}

// Snapshot freezes the counter, and returns all keys:counts
func (simpleGetKeyCounter *SimpleGetKeyCounter) Snapshot() map[string]uint64 {
	simpleGetKeyCounter.m.Lock()
	defer simpleGetKeyCounter.m.Unlock()
	// this is a readonly map
	readOnly := make(map[string]uint64)
	for k, c := range simpleGetKeyCounter.counts {
		readOnly[k] = c
	}
	return readOnly
}

// NewSimpleGetKeyCounter initializes a `SimpleGetKeyCounter`
func NewSimpleGetKeyCounter() *SimpleGetKeyCounter {
	return &SimpleGetKeyCounter{
		m:      sync.Mutex{},
		counts: map[string]uint64{},
	}
}

// BucketGetKeyCounter has buckets of `SimpleGetKeyCounter` mapped using its hashing function
type BucketGetKeyCounter struct {
	m           sync.RWMutex
	bucketing   *Bucketing
	snapshotted map[string]uint64
	freeze      int32
}

// Increment hashes the key to a bucket and delegates that
func (bucketGetKeyCounter *BucketGetKeyCounter) Increment(key string, delta uint64) {
	if atomic.LoadInt32(&bucketGetKeyCounter.freeze) == 0 {
		if counter, ok := bucketGetKeyCounter.bucketing.Pick([]byte(key)).(*SimpleGetKeyCounter); ok {
			counter.Increment(key, delta)
		}
	}
}

// Snapshot aggregates all buckets
func (bucketGetKeyCounter *BucketGetKeyCounter) Snapshot() map[string]uint64 {
	if atomic.LoadInt32(&bucketGetKeyCounter.freeze) == 0 {
		bucketGetKeyCounter.m.Lock()
		defer bucketGetKeyCounter.m.Unlock()
		if atomic.LoadInt32(&bucketGetKeyCounter.freeze) == 0 {
			aggregate := map[string]uint64{}
			bucketGetKeyCounter.bucketing.Each(func(counter interface{}) {
				if simpleGetKeyCounter, ok := counter.(*SimpleGetKeyCounter); ok {
					for k, c := range simpleGetKeyCounter.Snapshot() {
						aggregate[k] += c
					}
				}
			})
			bucketGetKeyCounter.snapshotted = aggregate
			atomic.StoreInt32(&bucketGetKeyCounter.freeze, 1)
			return aggregate
		}
	}

	bucketGetKeyCounter.m.RLock()
	defer bucketGetKeyCounter.m.RUnlock()
	return bucketGetKeyCounter.snapshotted
}

// NewBucketGetKeyCounter initializes a `BucketGetKeyCounter`
func NewBucketGetKeyCounter(buckets int) *BucketGetKeyCounter {
	return &BucketGetKeyCounter{
		m: sync.RWMutex{},
		bucketing: NewBucketing(func() interface{} {
			return NewSimpleGetKeyCounter()
		}, fnv.New32a, uint32(buckets)),
		snapshotted: map[string]uint64{},
		freeze:      0,
	}
}

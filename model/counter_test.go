package model

import (
	"reflect"
	"testing"
)

func TestEmptyGetKeyCounter(t *testing.T) {

	emptyKeyCounter := &EmptyGetKeyCounter{}
	if emptyKeyCounter.Snapshot() == nil || len(emptyKeyCounter.Snapshot()) != 0 {
		panic("empty key counter should always return empty snapshot")
	}

	emptyKeyCounter.Increment("some_key", 1)
	if emptyKeyCounter.Snapshot() == nil || len(emptyKeyCounter.Snapshot()) != 0 {
		panic("empty key counter should always return empty snapshot")
	}
}

func TestSimpleGetKeyCounter(t *testing.T) {
	simpleKeyCounter := NewSimpleGetKeyCounter()
	if simpleKeyCounter == nil || simpleKeyCounter.counts == nil || len(simpleKeyCounter.counts) != 0 {
		panic("simple key counter initialization incorrectly")
	}

	simpleKeyCounter.Increment("some_key", uint64(1))
	simpleKeyCounter.Increment("some_key", uint64(1))
	simpleKeyCounter.Increment("another_key", uint64(1))
	snapshot := simpleKeyCounter.Snapshot()
	if snapshot == nil || len(snapshot) != 2 || snapshot["some_key"] != uint64(2) || snapshot["another_key"] != uint64(1) {
		panic("simple key counter snapshot not right")
	}
	if !reflect.DeepEqual(snapshot, simpleKeyCounter.Snapshot()) {
		panic("simple key snapshot not freezed")
	}
}

func TestBucketGetKeyCounter(t *testing.T) {

	bucketKeyCounter := NewBucketGetKeyCounter(1)
	if bucketKeyCounter == nil || bucketKeyCounter.bucketing == nil {
		panic("bucket key counter initialization incorrectly")
	}

	bucketKeyCounter.Increment("some_key", uint64(1))
	bucketKeyCounter.Increment("some_key", uint64(1))
	bucketKeyCounter.Increment("another_key", uint64(1))
	snapshot := bucketKeyCounter.Snapshot()
	if snapshot == nil || len(snapshot) != 2 || snapshot["some_key"] != uint64(2) || snapshot["another_key"] != uint64(1) {
		panic("bucket key counter snapshot not right")
	}
	if !reflect.DeepEqual(snapshot, bucketKeyCounter.Snapshot()) {
		panic("bucket key snapshot not freezed")
	}
	if !reflect.DeepEqual(snapshot, bucketKeyCounter.Snapshot()) {
		panic("bucket key snapshot not freezed")
	}
}

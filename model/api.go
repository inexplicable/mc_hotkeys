package model

// GetKeyCounter is a 1 second bucket of keys GET occurances counter
// it's writtable when it's created, until it's snapshotted, then the counter will freeze
type GetKeyCounter interface {
	Increment(key string, delta uint64)
	Snapshot() map[string]uint64
}

// KeyScorer is a score giver for any given key
type KeyScorer interface {
	SetScore(key string, score uint64, exptime int64)
	DelScore(key ...string)
	GetScore(key string) uint64
}

// RollingWindows is a sequence of GetKeyCounter, and only the last one is writtable
// whenever it rolls, it creates a new `last` GetKeyCounter
// snapshot always combine all GetKeyCounter's snapshots except for the last one
type RollingWindows interface {
	last() GetKeyCounter
	Scorer() KeyScorer
	Roll() map[string]uint64
	Increment(key string, delta uint64)
}

// HotKeyReporter is a reporter of `RollingWindows` snapshot at a fixed interval
type HotKeyReporter interface {
	Report(map[string]uint64)
}

// HotKeyAggregator aggregates the reporters' subview into a consolidated view
type HotKeyAggregator interface {
	Aggregate() error
}

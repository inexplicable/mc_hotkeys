package model

import (
	"reflect"
	"testing"
)

type dumbKeyScorer struct {
}

func (dumbKeyScorer *dumbKeyScorer) GetScore(key string) uint64 {
	return uint64(1)
}

func (dumbKeyScorer *dumbKeyScorer) SetScore(key string, score uint64, exptime int64) {

}

func (dumbKeyScorer *dumbKeyScorer) DelScore(key ...string) {

}

func TestTopN(t *testing.T) {

	scorer := &dumbKeyScorer{}

	if !reflect.DeepEqual(topN(scorer, map[string]uint64{
		"some_key":        uint64(100),
		"some_hot_key":    uint64(1000),
		"another_hot_key": uint64(1001),
	}, 3, 101), map[string]uint64{
		"some_hot_key":    uint64(1000),
		"another_hot_key": uint64(1001),
	}) {
		panic("topN incorrect")
	}

	if !reflect.DeepEqual(topN(scorer, map[string]uint64{
		"some_key":        uint64(100),
		"some_hot_key":    uint64(1000),
		"another_hot_key": uint64(1001),
	}, 1, 101), map[string]uint64{
		"another_hot_key": uint64(1001),
	}) {
		panic("topN incorrect")
	}

	if !reflect.DeepEqual(topN(scorer, map[string]uint64{
		"some_key":        uint64(1001),
		"some_hot_key":    uint64(1000),
		"another_hot_key": uint64(100),
	}, 2, 0), map[string]uint64{
		"some_key":     uint64(1001),
		"some_hot_key": uint64(1000),
	}) {
		panic("topN incorrect")
	}
}

func TestSimpleRollingWindows(t *testing.T) {

	scorer := &dumbKeyScorer{}

	rollingWindows := NewSimpleRollingWindows(scorer, func() GetKeyCounter {
		return NewBucketGetKeyCounter(1)
	}, 4, 3, 1)
	if rollingWindows == nil || rollingWindows.width != 4 || rollingWindows.readFrom != 0 ||
		rollingWindows.readTo != 3 || rollingWindows.current != 4 || rollingWindows.windows == nil {
		panic("rolling windows initialization incorrect")
	}
	if rollingWindows.last() == nil {
		panic("rolling windows must have last writable window")
	}

	rollingWindows.Increment("some_key", uint64(1))
	rollingWindows.Increment("some_key", uint64(1))
	rollingWindows.Increment("another_key", uint64(1))
	if !reflect.DeepEqual(rollingWindows.Roll(), map[string]uint64{
		"some_key":    uint64(2),
		"another_key": uint64(1),
	}) {
		panic("rolling snapshot incorrect")
	}
	if rollingWindows.readFrom != 1 || rollingWindows.readTo != 4 || rollingWindows.current != 0 {
		panic("rolling windows state incorrect after 1st roll")
	}

	rollingWindows.Increment("some_key", uint64(1))
	rollingWindows.Increment("some_key", uint64(1))
	rollingWindows.Increment("another_key", uint64(1))
	if !reflect.DeepEqual(rollingWindows.Roll(), map[string]uint64{
		"some_key":    uint64(4),
		"another_key": uint64(2),
	}) {
		panic("rolling snapshot incorrect")
	}
	if rollingWindows.readFrom != 2 || rollingWindows.readTo != 0 || rollingWindows.current != 1 {
		panic("rolling windows state incorrect after 1st roll")
	}

	rollingWindows.Roll()
	if rollingWindows.readFrom != 3 || rollingWindows.readTo != 1 || rollingWindows.current != 2 {
		panic("rolling windows state incorrect after 1st roll")
	}

	rollingWindows.Roll()
	if rollingWindows.readFrom != 4 || rollingWindows.readTo != 2 || rollingWindows.current != 3 {
		panic("rolling windows state incorrect after 1st roll")
	}

	if !reflect.DeepEqual(rollingWindows.Roll(), map[string]uint64{
		"some_key":    uint64(2),
		"another_key": uint64(1),
	}) {
		panic("rolling snapshot incorrect")
	}
	if rollingWindows.readFrom != 0 || rollingWindows.readTo != 3 || rollingWindows.current != 4 {
		panic("rolling windows state incorrect after 1st roll")
	}
}

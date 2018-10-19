package model

import (
	"math/rand"
	"testing"
	"time"
)

func TestSimpleKeyScorer(t *testing.T) {

	minSlabBytes := uint64(96)
	scorer := NewSimpleKeyScorer(minSlabBytes, 1*time.Second)

	if scorer == nil || scorer.minBytes != minSlabBytes || scorer.trie == nil {
		panic("scorer creation failure")
	}

	if scorer.GetScore("anything") != minSlabBytes {
		panic("default score should be the minSlabBytes")
	}

	randomScore := uint64(rand.Int63n(int64(minSlabBytes) * 10))
	scorer.SetScore("some_key", randomScore, 0)
	if scorer.GetScore("some_key") != randomScore {
		panic("scorer should remember the score of a key")
	}

	higherScore := randomScore * 2
	scorer.SetScore("some_key", higherScore, 0)
	if scorer.GetScore("some_key") != higherScore {
		panic("scorer should remember a newer score")
	}

	scorer.DelScore("some_key")
	if scorer.GetScore("some_key") != minSlabBytes {
		panic("scorer should default to min after score was deleted")
	}
}

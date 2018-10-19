package model

import (
	"testing"
	"time"
)

func TestConsoleReporter(t *testing.T) {

	scorer := &dumbKeyScorer{}

	rollingWindows := NewSimpleRollingWindows(scorer, func() GetKeyCounter {
		return NewBucketGetKeyCounter(1)
	}, 4, 2, 1)
	reporter := NewLoggingHotKeyReporter(rollingWindows)

	if reporter == nil || reporter.rollingWindows == nil {
		panic("reporter not initialized correctly")
	}

	for tick := 0; tick < 3; tick++ {
		rollingWindows.Increment("some_key", uint64(1))
		rollingWindows.Increment("some_key", uint64(1))
		rollingWindows.Increment("another_key", uint64(1))
		time.Sleep(1 * time.Second)
	}
}

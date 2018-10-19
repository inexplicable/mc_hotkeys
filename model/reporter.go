package model

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	log "github.com/golang/glog"
)

// logHotKeyReporter reports to the console
type logHotKeyReporter struct {
	rollingWindows RollingWindows
}

// Report does the reporting
func (consoleGetKeyCountReporter *logHotKeyReporter) Report(content map[string]uint64) {
	log.Infof("<report> start: %v\n", time.Now())
	for k, score := range content {
		log.Infof("<report> %s:%d\n", k, score)
	}
}

// NewlogHotKeyReporter initializes the `ConsoleGetKeyReporter` and the `ticker` at every 1s
func NewlogHotKeyReporter(rollingWindows RollingWindows) *logHotKeyReporter {
	reporter := &logHotKeyReporter{
		rollingWindows: rollingWindows,
	}

	ticker := time.NewTicker(1 * time.Second)
	go func() {
		// roll & report every 1 second
		for range ticker.C {
			reporter.Report(rollingWindows.Roll())
		}
	}()
	return reporter
}

// MemcachedHotKeyReporter reports the topN keys to memcached key
type MemcachedHotKeyReporter struct {
	identity       string
	rollingWindows RollingWindows
	reportKey      string
	topN           int
	client         *memcache.Client
}

// Report does the report and uses json
func (memcachedGetKeyCountReporter *MemcachedHotKeyReporter) Report(updates map[string]uint64) {

	if rawBytes, err := json.Marshal(updates); err == nil {
		item := &memcache.Item{
			Key:   fmt.Sprintf("%s:%s", memcachedGetKeyCountReporter.reportKey, memcachedGetKeyCountReporter.identity),
			Value: rawBytes,
		}
		if err = memcachedGetKeyCountReporter.client.Set(item); err != nil {
			log.Warningf("<memcached report:%s> error :%v\n", memcachedGetKeyCountReporter.identity, err)
		} else {
			log.Infof("<memcached report:%s> done :%v\n", memcachedGetKeyCountReporter.identity, updates)
		}
	}
}

// ReporterIdentity is the default identity string of a reporter constructed from hostname and port
func ReporterIdentity(host string, port int) string {
	if host == "" {
		host, _ = os.Hostname()
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// NewMemcachedHotKeyReporter initializes the `MemcachedGetKeyCountReporter` using the given `memcached` hosts list
func NewMemcachedHotKeyReporter(rollingWindows RollingWindows, identity string, reportKey string, topN int, registry McrouterRegistry) *MemcachedHotKeyReporter {

	reporter := &MemcachedHotKeyReporter{
		identity:       identity,
		rollingWindows: rollingWindows,
		reportKey:      reportKey,
		topN:           topN,
		client:         memcache.NewFromSelector(registry),
	}

	ticker := time.NewTicker(1 * time.Second)
	go func() {
		// roll & report every 1 second
		for range ticker.C {
			log.Infof("<memcached report> start:%v\n", time.Now())
			reporter.Report(rollingWindows.Roll())
		}
	}()
	return reporter
}

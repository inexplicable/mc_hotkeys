package model

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	log "github.com/golang/glog"
	consul "github.com/hashicorp/consul/api"
)

// HotKeyEntry is needed for sorting
type HotKeyEntry struct {
	Key   string
	Score uint64
}

// HotKeyEntries is a slice of `HotKey` with heap interface, and it's maxheap
type HotKeyEntries []*HotKeyEntry

func (entries HotKeyEntries) Len() int           { return len(entries) }
func (entries HotKeyEntries) Less(i, j int) bool { return entries[i].Score > entries[j].Score }
func (entries HotKeyEntries) Swap(i, j int)      { entries[i], entries[j] = entries[j], entries[i] }

// Push is for heap interface
func (entries *HotKeyEntries) Push(x interface{}) {
	if entry, ok := x.(*HotKeyEntry); ok {
		*entries = append(*entries, entry)
	}
}

// Pop is for heap interface
func (entries *HotKeyEntries) Pop() interface{} {
	old := *entries
	n := len(old)
	item := old[n-1]
	*entries = old[0 : n-1]
	return item
}

// MemcachedHotKeyAggregator aggregates `MemcachedHotKeyReporter`'s reports
type MemcachedHotKeyAggregator struct {
	serviceName     string
	reportKey       string
	topN            int
	interval        int
	memcachedClient *memcache.Client
	consulClient    *consul.Client
}

func (memcachedHotKeyAggregator *MemcachedHotKeyAggregator) discover() []string {
	qo := &consul.QueryOptions{
		AllowStale:        true,
		RequireConsistent: false,
	}

	reporters, _, err := memcachedHotKeyAggregator.consulClient.Health().Service(memcachedHotKeyAggregator.serviceName, "", true, qo)
	if err != nil {
		log.Warningf("<aggregator> discover `mc_guardian` service failed")
		reporters = []*consul.ServiceEntry{}
	}

	reporterKeys := make([]string, 0, len(reporters))
	for _, reporter := range reporters {
		reporterKeys = append(reporterKeys, fmt.Sprintf("%s:%s", memcachedHotKeyAggregator.reportKey, reporter.Node.Address))
	}
	return reporterKeys
}

func (memcachedHotKeyAggregator *MemcachedHotKeyAggregator) elect(interval int) error {

	lockKey := fmt.Sprintf("%s:%s:leader", memcachedHotKeyAggregator.serviceName, memcachedHotKeyAggregator.reportKey)
	lockOpt := &consul.LockOptions{Key: lockKey}
	if locker, err := memcachedHotKeyAggregator.consulClient.LockOpts(lockOpt); err == nil {
		for {
			// blocks till leadership is acquired
			if leader, err := locker.Lock(nil); err == nil {
				ticker := time.NewTicker(time.Duration(interval) * time.Second)
				for {
					<-ticker.C // wait till next tick
					select {   // check if leadership is still in possesion
					case _, open := <-leader:
						if !open {
							log.Infof("<memcached aggregator> leadership lost:%v\n", time.Now())
							ticker.Stop()
							break
						}
					}
					log.Infof("<memcached aggregator> start:%v\n", time.Now())
					memcachedHotKeyAggregator.Aggregate()
				}
			} else {
				log.Warningf("<memcached aggregator> recover from leadership election errror:%v\n", err)
			}
		}
	} else {
		log.Errorf("<memcached aggregator> stops leadership election due to error:%v\n", err)
		return err
	}
}

// Aggregate aggregates reports from all repoters and take the highest topN subset
func (memcachedHotKeyAggregator *MemcachedHotKeyAggregator) Aggregate() error {

	reporterKeys := memcachedHotKeyAggregator.discover()
	if len(reporterKeys) == 0 {
		return nil
	}
	reports, err := memcachedHotKeyAggregator.memcachedClient.GetMulti(reporterKeys)
	if err != nil {
		return err
	}
	hotKeyEntries := HotKeyEntries{}
	for _, report := range reports {
		candidates := map[string]uint64{}
		if err = json.Unmarshal(report.Value, &candidates); err == nil {
			for key, score := range candidates {
				hotKeyEntries = append(hotKeyEntries, &HotKeyEntry{key, score})
			}
		}
	}
	// topN is likely to be small, will switch to `container/heap` for better efficiency
	heap.Init(&hotKeyEntries)
	cutN := HotKeyEntries(make([]*HotKeyEntry, 0, memcachedHotKeyAggregator.topN))
	for t := 0; t < memcachedHotKeyAggregator.topN && hotKeyEntries.Len() > 0; t++ {
		if top, ok := heap.Pop(&hotKeyEntries).(*HotKeyEntry); ok {
			cutN = append(cutN, top)
		}
	}
	hotKeysRawBytes, err := json.Marshal(cutN)
	if err != nil {
		return err
	}
	aggregateItem := &memcache.Item{
		Key:   memcachedHotKeyAggregator.reportKey,
		Value: hotKeysRawBytes,
	}
	return memcachedHotKeyAggregator.memcachedClient.Set(aggregateItem)
}

// NewMemcachedHotKeyAggregator initializes a `MemcachedHotKeyAggregator`
func NewMemcachedHotKeyAggregator(serviceName, reportKey string, topN int, interval int, registry McrouterRegistry) *MemcachedHotKeyAggregator {

	memcachedClient := memcache.NewFromSelector(registry)
	consulClient, err := NewConsulClient()
	if err != nil {
		log.Errorf("cannot get consul client due to %v\n", err)
		return nil
	}

	aggregator := &MemcachedHotKeyAggregator{
		serviceName:     serviceName,
		reportKey:       reportKey,
		topN:            topN,
		memcachedClient: memcachedClient,
		consulClient:    consulClient,
	}

	go func() {
		aggregator.elect(interval)
	}()
	return aggregator
}

package main

import (
	"flag"
	"fmt"
	"net"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	log "github.com/golang/glog"
	"github.com/inexplicable/mc_hotkeys/mcrouter"
	"github.com/inexplicable/mc_hotkeys/model"
)

var (
	host         = flag.String("host", "", "listing host name for incoming memcached text connection")
	port         = flag.Int("port", 11211, "listening port for incoming memcached text connections")
	rollingWidth = flag.Int("rolling_width", 10, "number of rolling windows (each is 1s), default 10s")
	topN         = flag.Int("top_n", 10, "number of top hot keys to be reported")
	threshold    = flag.Uint64("threshold", 100, "mininal number of requests in the aggregate windows")
	minSlabBytes = flag.Uint64("min_slab_bytes", 96, "chunk size(bytes) of the smallest slab")
	mcrouterPort = flag.Int("mcrouter_port", 8989, "known mcrouter port")
	memcachedKey = flag.String("memcached_key", "MEMCACHED_HOT_KEYS", "memcached key of the hot keys")
	serviceName  = flag.String("service_name", "mc_hotkeys", "consul service name")
	secretsPath  = flag.String("secrets_path", "/etc/consul/mc_hotkeys.json", "vault secrets path")
)

func newEavesdropper() (model.RollingWindows, mcrouter.Eavesdropper) {
	buckets := (1 + runtime.NumCPU()) * 4 // at least 4 buckets
	scorer := model.NewBucketKeyScorer(buckets, *minSlabBytes, time.Duration(*rollingWidth)*time.Minute)
	rollingWindows := model.NewSimpleRollingWindows(scorer, func() model.GetKeyCounter {
		return model.NewBucketGetKeyCounter(buckets)
	}, *rollingWidth, *topN, *threshold)
	return rollingWindows, mcrouter.NewRollingWindowsMcrouterEavesdropper(rollingWindows, scorer)
}

func main() {
	// parse the flags
	flag.Parse()

	// Listen for incoming connections.
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		log.Errorf("cannot start listener due to:%v", err)
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	log.Infof("eavesdropper starts on %s:%d, rolling width:%d, topN:%d, threshold:%d\n", *host, *port, *rollingWidth, *topN, *threshold)

	notFound := model.ReadEvery(*secretsPath, 10*time.Minute)
	rollingWindows, eavesdropper := newEavesdropper()
	mcrouterRegistry := model.NewMcrouterRegistry(*mcrouterPort)
	model.NewMemcachedHotKeyReporter(rollingWindows, model.ReporterIdentity(*host, *port), *memcachedKey, *topN, mcrouterRegistry)
	if notFound == nil {
		model.NewMemcachedHotKeyAggregator(*serviceName, *memcachedKey, *topN, *rollingWidth, mcrouterRegistry)
	}

	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			log.Warningf("error accepting connection:%v\n", err)
		} else {
			remoteAddr := conn.RemoteAddr()
			log.Infof("accepted connection from:%v\n", remoteAddr)
			mcrouterRegistry.Register(remoteAddr.String())
			// Handle connections in a new goroutine.
			go func() {
				defer mcrouterRegistry.Unregister(remoteAddr.String())
				mcrouter.Serve(conn, eavesdropper)
			}()
		}
	}
}

package model

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/bradfitz/gomemcache/memcache"
)

// ErrParseMcrouter is an error when parsing mcrouter address string
var ErrParseMcrouter = errors.New("mcrouter parse error")

// McrouterRegistry registers/lists mcrouter hosts known from the connections
type McrouterRegistry interface {
	memcache.ServerSelector
	Register(mcrouter string) error
	Unregister(mcrouter string) error
}

// SimpleMcrouterRegistry is a implementer of McrouterRegistry
type SimpleMcrouterRegistry struct {
	m            sync.Mutex
	ss           *memcache.ServerList
	mcrouterPort int
	mcrouters    map[string]int
}

// Register tries registering a candidate `mcrouter`, it actually tests if a tcp `version\r\n` request gets the proper response back.
func (simpleMcrouterRegistry *SimpleMcrouterRegistry) Register(mcrouter string) error {
	simpleMcrouterRegistry.m.Lock()
	defer simpleMcrouterRegistry.m.Unlock()

	if _, ok := simpleMcrouterRegistry.mcrouters[mcrouter]; ok {
		return nil
	}

	if testing, err := parseMcrouter(mcrouter, simpleMcrouterRegistry.mcrouterPort); err == nil && testMcrouter(testing) {
		simpleMcrouterRegistry.mcrouters[testing]++
		simpleMcrouterRegistry.unsafeUpdateServerList()
		return nil
	}
	return ErrParseMcrouter
}

// Unregister reduces the `usages` of a mcrouter
func (simpleMcrouterRegistry *SimpleMcrouterRegistry) Unregister(mcrouter string) error {
	simpleMcrouterRegistry.m.Lock()
	defer simpleMcrouterRegistry.m.Unlock()

	if _, ok := simpleMcrouterRegistry.mcrouters[mcrouter]; !ok {
		return nil
	}

	if parsed, err := parseMcrouter(mcrouter, simpleMcrouterRegistry.mcrouterPort); err == nil {
		simpleMcrouterRegistry.mcrouters[parsed]--
		simpleMcrouterRegistry.unsafeUpdateServerList()
		return nil
	}
	return ErrParseMcrouter
}

func (simpleMcrouterRegistry *SimpleMcrouterRegistry) unsafeUpdateServerList() {
	servers := make([]string, 0, len(simpleMcrouterRegistry.mcrouters))
	for m, usages := range simpleMcrouterRegistry.mcrouters {
		if usages > 0 {
			servers = append(servers, m)
		}
	}
	simpleMcrouterRegistry.ss.SetServers(servers...)
}

func parseMcrouter(mcrouter string, mcrouterPort int) (string, error) {
	if colon := strings.LastIndex(mcrouter, ":"); colon > 0 && colon < len(mcrouter)-1 {
		host := mcrouter[0:colon]
		if _, err := strconv.Atoi(mcrouter[colon+1:]); err == nil {
			return fmt.Sprintf("%s:%d", host, mcrouterPort), nil
		}
	}
	return "", ErrParseMcrouter
}

// PickServer gives a set of `mcrouter` hosts registered
func (simpleMcrouterRegistry *SimpleMcrouterRegistry) PickServer(key string) (net.Addr, error) {
	return simpleMcrouterRegistry.ss.PickServer(key)
}

// Each iterates each mcrouter
func (simpleMcrouterRegistry *SimpleMcrouterRegistry) Each(f func(net.Addr) error) error {
	return simpleMcrouterRegistry.ss.Each(f)
}

func testMcrouter(candidate string) bool {
	log.Infof("<discovery> testing:%s for being a mcrouter\n", candidate)
	if addr, err := net.ResolveTCPAddr("tcp", candidate); err == nil {
		if conn, err := net.DialTCP("tcp", nil, addr); err == nil {
			defer conn.Close()
			if err = conn.SetDeadline(time.Now().Add(1 * time.Second)); err == nil {
				if _, err = conn.Write([]byte("version\r\n")); err == nil {
					buf := make([]byte, 128)
					if read, err := conn.Read(buf); err == nil {
						// VERSION 36.0.0-master mcrouter
						if strings.HasSuffix(strings.TrimSpace(string(buf[0:read])), "mcrouter") {
							return true
						}
					}
				}
			}
		}
	}
	log.Warningf("<discovery> %s failed the test of mcrouter\n", candidate)
	return false
}

// NewMcrouterRegistry creates a `SimpleMcrouterRegistry` instance
func NewMcrouterRegistry(port int) McrouterRegistry {
	return &SimpleMcrouterRegistry{
		m:            sync.Mutex{},
		ss:           &memcache.ServerList{},
		mcrouterPort: port,
		mcrouters:    map[string]int{},
	}
}

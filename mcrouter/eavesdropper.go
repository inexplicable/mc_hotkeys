package mcrouter

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/inexplicable/mc_hotkeys/model"
)

// ErrQuit is an error for `quit` command
var ErrQuit = errors.New("quit")

// Eavesdropper is a server alike of `Memcached` text protocol
// it parses the command received from a memcached client, and handles only `GET(s)` & `SET,ADD,REPLACE`
type Eavesdropper interface {
	MemcachedServer
}

// AbstractMcrouterEavesdropper handles `OnFetch|OnStore|OnDelete` and integrates with `OnCommand` interface
type AbstractMcrouterEavesdropper struct {
	OnFetch  func(keys ...string)
	OnStore  func(key string, len int, exptime int64)
	OnDelete func(key string)
}

// OnCommand dispatches and handles response, errors from `OnFetch|OnStore|OnDelete`
func (eavesdropper *AbstractMcrouterEavesdropper) OnCommand(command Command, args []string, scanner *bufio.Scanner) ([]byte, error) {

	switch command {
	case GET, GETS, GAT, GATS:
		eavesdropper.OnFetch(args...)
		return End, nil
	case SET, ADD, REPLACE, CAS:
		if key, bytes, exptime, err := parseStore(args); err == nil {
			eavesdropper.OnStore(key, bytes, exptime)
			skipN(scanner, bytes+2)
		}
		return NotStored, nil
	case DELETE:
		if len(args) > 0 {
			eavesdropper.OnDelete(args[0])
		}
		return NotFound, nil
	case APPEND, PREPEND, INCR, DECR, TOUCH:
		// cheat by saying `NOT_FOUND`
		return NotFound, nil
	case VERSION:
		// cheat by give a dumb version string
		return Version, nil
	case STATS:
		// cheat by saying `END` immediately
		return End, nil
	case QUIT:
		// exit immediately
		return nil, ErrQuit
	default:
		// rare cases, cheat by saying it's a client error
		return ClientError, nil
	}
}

// NoopMcrouterEavesdropper is a noop eavesdropper mainly for embedding
type NoopMcrouterEavesdropper struct {
	AbstractMcrouterEavesdropper
}

// NewNoopMcrouterEavesdropper initialize a NoopMcrouterEavesdropper
func NewNoopMcrouterEavesdropper() Eavesdropper {
	return &NoopMcrouterEavesdropper{
		AbstractMcrouterEavesdropper: AbstractMcrouterEavesdropper{
			OnFetch:  func(keys ...string) {},
			OnStore:  func(key string, len int, exptime int64) {},
			OnDelete: func(key string) {},
		},
	}
}

// RollingWindowsMcrouterEavesdropper is an eavesdropper that actually does the counting, scoring etc.
type RollingWindowsMcrouterEavesdropper struct {
	AbstractMcrouterEavesdropper
	rollingWindows model.RollingWindows
	keyScorer      model.KeyScorer
}

// OnFetch increments the count of the `keys`
func (eavesdropper *RollingWindowsMcrouterEavesdropper) OnFetch(keys ...string) {
	for _, key := range keys {
		eavesdropper.rollingWindows.Increment(key, uint64(1))
	}
}

// OnStore sets the score of the key using its bytes length
func (eavesdropper *RollingWindowsMcrouterEavesdropper) OnStore(key string, len int, exptime int64) {
	eavesdropper.keyScorer.SetScore(key, uint64(len), exptime)
}

// OnDelete removes the score of the key using its bytes length
func (eavesdropper *RollingWindowsMcrouterEavesdropper) OnDelete(key string) {
	eavesdropper.keyScorer.DelScore(key)
}

// NewRollingWindowsMcrouterEavesdropper initializes a `RollingWindowsMcrouterEavesdropper`
func NewRollingWindowsMcrouterEavesdropper(rollingWindows model.RollingWindows, keyScorer model.KeyScorer) *RollingWindowsMcrouterEavesdropper {

	eavesdropper := &RollingWindowsMcrouterEavesdropper{
		rollingWindows: rollingWindows,
		keyScorer:      keyScorer,
	}

	eavesdropper.AbstractMcrouterEavesdropper = AbstractMcrouterEavesdropper{
		OnFetch: func(keys ...string) {
			eavesdropper.OnFetch(keys...)
		},
		OnStore: func(key string, len int, exptime int64) {
			eavesdropper.OnStore(key, len, exptime)
		},
		OnDelete: func(key string) {
			eavesdropper.OnDelete(key)
		},
	}

	return eavesdropper
}

// Eavesdropping on mcrouter `GET|GETS|SET|ADD|CAS|REPLACE|DELETE` commands by using `mcrouter` routing
// the routing policy looks like this:
/*
{
  "macros": {
    "createAllInitialRoute": {
      "type": "macroDef",
      "params": [ "main", "other" ],
      "result": {
        "type": "AllInitialRoute",
        "children": ["PoolRoute|%main%", "PoolRoute|%other%"]
      }
	},
    "eavesdropping": {
      "type": "macroDef",
      "params": [ "main", "other" ],
      "result": {
        "type": "OperationSelectorRoute",
        "default_policy": "PoolRoute|%main%",
        "operation_policies": {
          "get": "@createAllInitialRoute(%main%, %other%)",
          "add": "@createAllInitialRoute(%main%, %other%)",
          "set": "@createAllInitialRoute(%main%, %other%)",
          "delete": "@createAllInitialRoute(%main%, %other%)",
        }
      }
    }
  },
  "pools": {
},
  "route": {
    "type": "PrefixSelectorRoute",
    "policies": {
        "PREFIX": "@eavesdropping(primary, memc-hotkeys)"
    },
    // All keys without a known prefix go to the default pool
    "wildcard": "@eavesdropping(secondary, memc-hotkeys)"
    }
}
}
*/

func skipN(scanner *bufio.Scanner, n int) error {
	for scanner.Scan() {
		scanned := len(scanner.Bytes()) + 2 // CRLF
		n -= scanned
		if n < 0 { // premature EOF
			return ErrParse
		}
		if n == 0 { // exact n bytes read
			return nil
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}
	return ErrParse
}

func parseStore(args []string) (string, int, int64, error) {
	if len(args) < 4 {
		return "", 0, 0, ErrParse
	}
	bytes, err := strconv.Atoi(strings.TrimSpace(args[3]))
	if err != nil {
		return "", 0, 0, ErrParse
	}
	exptime, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return "", 0, 0, ErrParse
	}
	if exptime > 0 && exptime <= MAX_EXPIRE_SECONDS {
		exptime = time.Now().Unix() + exptime
	}
	return args[0], bytes, exptime, nil
}

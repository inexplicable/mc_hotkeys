package mcrouter

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"strings"
)

// Command is an enum of Memcached COMMAND eavesdropper knows
type Command int

const (
	// UNKNOWN is a command ignored by eavesdropper
	UNKNOWN Command = iota
	GET
	GETS
	GAT
	GATS
	SET
	ADD
	REPLACE
	CAS
	APPEND
	PREPEND
	INCR
	DECR
	TOUCH
	DELETE
	STATS
	VERSION
	QUIT
)

// CRLF is the line delimiter
const CRLF = "\r\n"

// MAX_EXPIRE_SECONDS in seconds, beyond this value, the given `exptime` is deemed as a unix timestamp
const MAX_EXPIRE_SECONDS = 60 * 60 * 24 * 30

var (
	// Version is the only response to `VERSION` query, only needed for completeness
	Version = []byte("mc_guardian 0.1\r\n")
	// End is always the response to `GET/GETS`
	End = []byte("END\r\n")
	// NotStored is always the response to `SET/ADD/REPLACE`
	NotStored = []byte("NOT_STORED\r\n")
	// NotFound is always the response to `DELETE`
	NotFound = []byte("NOT_FOUND\r\n")
	// ClientError is always the response to any other command other than abovementioned
	ClientError = []byte("CLIENT_ERROR <ignore eavesdropping error>\r\n")
	// ErrParse is sth wrong detected in the command parsing
	ErrParse = errors.New("command_parse_error")
)

func scanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if i := bytes.IndexAny(data, CRLF); i >= 0 {
		// We have a full newline-terminated line.
		return i + 2, data[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

// MemcachedServer is a server alike of `Memcached` text protocol
// it parses the command received from a memcached client, and handles only `GET(s)` & `SET,ADD,REPLACE`
type MemcachedServer interface {
	OnCommand(command Command, args []string, scanner *bufio.Scanner) ([]byte, error)
}

func Serve(conn net.Conn, memcachedServer MemcachedServer) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	scanner.Split(scanCRLF)

	for scanner.Scan() {
		if cmd, args, err := parseCommand(scanner.Text()); err == nil {
			if resp, err := memcachedServer.OnCommand(cmd, args, scanner); err == nil {
				if _, err := conn.Write(resp); err == nil {
					continue
				}
			}
		}
		// there's some error we couldn't continue scan
		break
	}
}

func parseCommand(command string) (Command, []string, error) {
	sections := strings.Split(command, " ")
	if len(sections) < 1 {
		return UNKNOWN, nil, ErrParse
	}

	switch cmd := sections[0]; cmd {
	case "get":
		return GET, sections[1:], nil
	case "gets":
		return GETS, sections[1:], nil
	case "gat":
		return GAT, sections[2:], nil
	case "gats":
		return GATS, sections[2:], nil
	case "set":
		return SET, sections[1:], nil
	case "add":
		return ADD, sections[1:], nil
	case "replace":
		return REPLACE, sections[1:], nil
	case "delete":
		return DELETE, sections[1:], nil
	case "cas":
		return CAS, sections[1:], nil
	case "append":
		return APPEND, sections[1:], nil
	case "prepend":
		return PREPEND, sections[1:], nil
	case "incr":
		return INCR, sections[1:], nil
	case "decr":
		return DECR, sections[1:], nil
	case "touch":
		return TOUCH, sections[1:], nil
	case "stats":
		return STATS, sections[1:], nil
	case "version":
		return VERSION, []string{}, nil
	case "quit":
		return QUIT, []string{}, nil
	default:
		return UNKNOWN, nil, nil
	}
}

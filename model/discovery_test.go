package model

import (
	"net"
	"testing"
)

func TestMcrouterRegistry(t *testing.T) {

	registry := NewMcrouterRegistry(8989)
	if registry == nil {
		panic("failed to initialize mcrouter registry")
	}

	if _, err := registry.PickServer("some key"); err == nil {
		panic("pick server should get error as there's no mcrouter yet")
	}

	if registry.Register("localhost:10000") == nil {
		panic("mcrouter test should fail and block this registration")
	}

	if l, err := net.Listen("tcp", "localhost:8989"); err != nil {
		panic("cannot start fake mcrouter")
	} else {
		go (func() {
			for {
				// Listen for an incoming connection.
				conn, _ := l.Accept()
				buf := make([]byte, len("version\r\n"))
				conn.Read(buf)
				conn.Write([]byte("VERSION 36.0.0-master mcrouter\r\n"))
				conn.Close()
			}
		})()
	}

	if registry.Register("localhost:10000") != nil {
		panic("mcrouter test should have succeeded")
	}
	if addr, err := registry.PickServer("some_key"); err != nil || addr.Network() != "tcp" {
		panic("mcrouter registry shoudl serve localhost:8989 but got:" + addr.String())
	}
}

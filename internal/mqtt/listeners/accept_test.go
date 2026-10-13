package listeners

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// flakyListener fails its first Accept, like a process that hit its fd limit (EMFILE).
type flakyListener struct {
	net.Listener
	failed atomic.Bool
}

func (f *flakyListener) Accept() (net.Conn, error) {
	if f.failed.CompareAndSwap(false, true) {
		return nil, errors.New("accept tcp: too many open files")
	}
	return f.Listener.Accept()
}

// acceptListeners builds each Accept-loop listener on top of ln.
func acceptListeners(ln net.Listener) map[string]Listener {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	tcp := NewTCP(Config{ID: "tcp"})
	tcp.listen, tcp.log = ln, log
	unix := NewUnixSock(Config{ID: "unix"})
	unix.listen, unix.log = ln, log
	nl := NewNet("net", ln)
	nl.log = log
	return map[string]Listener{"tcp": tcp, "unix": unix, "net": nl}
}

func TestListenersKeepAcceptingAfterAcceptError(t *testing.T) {
	for _, name := range []string{"tcp", "unix", "net"} {
		t.Run(name, func(t *testing.T) {
			inner, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			l := acceptListeners(&flakyListener{Listener: inner})[name]

			established := make(chan struct{}, 1)
			served := make(chan struct{})
			go func() {
				l.Serve(func(string, net.Conn) error { established <- struct{}{}; return nil })
				close(served)
			}()

			conn, err := net.Dial("tcp", inner.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()

			select {
			case <-established:
			case <-served:
				t.Fatal("Serve returned after a transient Accept error")
			case <-time.After(2 * time.Second):
				t.Fatal("connection after a transient Accept error was never established")
			}

			l.Close(MockCloser)
			select {
			case <-served:
			case <-time.After(2 * time.Second):
				t.Fatal("Serve did not return after Close")
			}
		})
	}
}

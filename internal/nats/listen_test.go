package nats

import (
	"net"
	"testing"
)

func TestNewBindsConfiguredAddress(t *testing.T) {
	s, err := New(Deps{Address: "127.0.0.1", Port: 0})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()

	addr := s.listener.Addr().(*net.TCPAddr)
	if !addr.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("listener bound to %s, want 127.0.0.1", addr.IP)
	}
	if s.Port() == 0 {
		t.Fatal("expected an assigned port")
	}
}

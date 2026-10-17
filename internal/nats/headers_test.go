package nats

import (
	"strings"
	"testing"

	"monstermq.io/edge/internal/stores"
)

func TestParseHeaders(t *testing.T) {
	block := "NATS/1.0\r\nFoo: Bar\r\nBaz: Qux\r\nFoo: IgnoreMe\r\n\r\n"
	props := parseHeaders(block)
	if len(props) != 2 {
		t.Fatalf("expected 2 properties, got %d", len(props))
	}
	if props[0].Key != "Foo" || props[0].Value != "Bar" {
		t.Errorf("expected Foo: Bar, got %+v", props[0])
	}
	if props[1].Key != "Baz" || props[1].Value != "Qux" {
		t.Errorf("expected Baz: Qux, got %+v", props[1])
	}
}

func TestFormatHeaders(t *testing.T) {
	props := []stores.UserProperty{
		{Key: "Foo", Value: "First"},
		{Key: "Baz", Value: "Qux"},
		{Key: "Foo", Value: "Second"},
	}
	out := formatHeaders("", props, nil)
	if !strings.HasPrefix(out, "NATS/1.0\r\n") {
		t.Errorf("expected prefix NATS/1.0, got %q", out)
	}
	if !strings.Contains(out, "Foo: Second\r\n") {
		t.Errorf("expected Foo: Second in output, got %q", out)
	}
	if !strings.Contains(out, "Baz: Qux\r\n") {
		t.Errorf("expected Baz: Qux in output, got %q", out)
	}
	if strings.Count(out, "Foo:") != 1 {
		t.Errorf("expected duplicate Foo collapsed, got %q", out)
	}
}

func TestFormatHeadersStatus(t *testing.T) {
	out := formatHeadersMap("100 Idle Heartbeat", mapOf("Nats-Last-Consumer", "1"))
	if !strings.HasPrefix(out, "NATS/1.0 100 Idle Heartbeat\r\n") {
		t.Errorf("expected status header, got %q", out)
	}
}

func mapOf(k, v string) map[string]string {
	return map[string]string{k: v}
}

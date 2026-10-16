package auth

import (
	"fmt"
	"strings"
	"testing"

	"monstermq.io/edge/internal/stores"
)

func memoCache(t testing.TB, aclCheckOnSubscribe bool, rules ...stores.AclRule) (*Cache, *fakeUserStore) {
	t.Helper()
	for i := range rules {
		rules[i].Username = "alice"
	}
	store := &fakeUserStore{
		users: []stores.User{{Username: "alice", Enabled: true, CanPublish: true, CanSubscribe: true}},
		rules: rules,
	}
	c := NewCache(store, false, aclCheckOnSubscribe)
	if err := c.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	return c, store
}

// Delivery-time checks run per message per subscriber; a repeat must be a map hit.
func TestRepeatedAllowClientIsMemoized(t *testing.T) {
	c, _ := memoCache(t, false, stores.AclRule{TopicPattern: "machine/#", CanPublish: true, CanSubscribe: true})
	c.AllowClient("alice", "c1", "machine/a/b", false)
	if allocs := testing.AllocsPerRun(100, func() {
		c.AllowClient("alice", "c1", "machine/a/b", false)
	}); allocs != 0 {
		t.Fatalf("repeat AllowClient allocated %v times, want a memoized hit (0)", allocs)
	}
}

func TestRefreshInvalidatesMemoizedDecisions(t *testing.T) {
	c, store := memoCache(t, true, stores.AclRule{TopicPattern: "machine/#", CanPublish: true, CanSubscribe: true})
	if !c.AllowClient("alice", "c1", "machine/a", true) {
		t.Fatal("want allowed before the rule change")
	}
	store.rules = []stores.AclRule{{Username: "alice", TopicPattern: "machine/#", Priority: 10}}
	if err := c.Refresh(t.Context()); err != nil {
		t.Fatal(err)
	}
	if c.AllowClient("alice", "c1", "machine/a", true) {
		t.Fatal("decision survived Refresh with a deny rule added")
	}
}

// %c rules make the verdict depend on the client id, so it is part of the key.
func TestMemoKeysOnClientID(t *testing.T) {
	c, _ := memoCache(t, true, stores.AclRule{TopicPattern: "edge/%c/#", CanPublish: true, CanSubscribe: true})
	if !c.AllowClient("alice", "a", "edge/a/x", true) {
		t.Fatal("client a should reach its own namespace")
	}
	if c.AllowClient("alice", "b", "edge/a/x", true) {
		t.Fatal("client b got client a's memoized grant")
	}
}

func TestMemoIsBounded(t *testing.T) {
	c, _ := memoCache(t, true, stores.AclRule{TopicPattern: "machine/#", CanPublish: true, CanSubscribe: true})
	for i := 0; i < maxACLDecisions+10; i++ {
		c.AllowClient("alice", "c1", "machine/"+fmt.Sprint(i), true)
	}
	if n := len(c.decisions); n > maxACLDecisions {
		t.Fatalf("memo holds %d decisions, cap is %d", n, maxACLDecisions)
	}
}

// Topics may be up to 64 KiB; memoizing those would let the entry cap hold gigabytes.
func TestLongTopicsAreNotMemoized(t *testing.T) {
	c, _ := memoCache(t, true, stores.AclRule{TopicPattern: "machine/#", CanPublish: true, CanSubscribe: true})
	c.AllowClient("alice", "c1", "machine/"+strings.Repeat("x", 1024), true)
	if n := len(c.decisions); n != 0 {
		t.Fatalf("memoized %d decisions for an oversized topic, want 0", n)
	}
}

func BenchmarkAllowClientDelivery(b *testing.B) {
	c, _ := memoCache(b, false,
		stores.AclRule{TopicPattern: "site/+/cfg", CanPublish: true, Priority: 10},
		stores.AclRule{TopicPattern: "edge/%c/#", CanPublish: true, CanSubscribe: true, Priority: 5},
		stores.AclRule{TopicPattern: "machine/#", CanPublish: true, CanSubscribe: true, Priority: 1},
	)
	topics := make([]string, 2000)
	for i := range topics {
		topics[i] = fmt.Sprintf("machine/line%d/cell%d/value", i%20, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AllowClient("alice", "c1", topics[i%len(topics)], false)
	}
}

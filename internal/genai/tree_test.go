package genai

import (
	"strings"
	"testing"
)

func TestFormatTopicValue(t *testing.T) {
	// Empty payload
	val, ok := FormatTopicValue([]byte{}, 100)
	if !ok || val != "" {
		t.Fatalf("expected empty string and ok=true, got %q, %v", val, ok)
	}

	// Normal string
	val, ok = FormatTopicValue([]byte("hello world"), 100)
	if !ok || val != "hello world" {
		t.Fatalf("expected 'hello world', got %q, %v", val, ok)
	}

	// Truncation when maxLength > 0
	val, ok = FormatTopicValue([]byte("1234567890"), 5)
	if !ok || val != "12345...[truncated]" {
		t.Fatalf("expected '12345...[truncated]', got %q", val)
	}

	// Unlimited length (maxLength <= 0)
	val, ok = FormatTopicValue([]byte("1234567890"), 0)
	if !ok || val != "1234567890" {
		t.Fatalf("expected untruncated string, got %q", val)
	}

	// Invalid UTF-8
	_, ok = FormatTopicValue([]byte{0xff, 0xfe, 0xfd}, 100)
	if ok {
		t.Fatalf("expected ok=false for invalid UTF-8")
	}

	// Control character (binary)
	_, ok = FormatTopicValue([]byte("hello\x01world"), 100)
	if ok {
		t.Fatalf("expected ok=false for binary with control characters")
	}

	// Permitted whitespace control characters
	val, ok = FormatTopicValue([]byte("hello\n\t\rworld"), 100)
	if !ok || val != "hello\n\t\rworld" {
		t.Fatalf("expected whitespace control characters to be allowed, got %q, %v", val, ok)
	}

	// Exceeds 5MB guard when unlimited
	huge := make([]byte, 6*1024*1024)
	for i := range huge {
		huge[i] = 'a'
	}
	_, ok = FormatTopicValue(huge, 0)
	if ok {
		t.Fatalf("expected ok=false for payload exceeding 5MB")
	}
}

func TestFormatTopicsAsHierarchy(t *testing.T) {
	topics := map[string]string{
		"ProveIt/Enterprise B/Site1/node/assetid": "1",
		"ProveIt/Enterprise B/Site1/node/name":    "Site1",
		"ProveIt/Enterprise B/Site2/node/assetid": "2",
	}

	tree := FormatTopicsAsHierarchy(topics)
	if !strings.Contains(tree, "ProveIt/") {
		t.Errorf("expected tree to contain 'ProveIt/', got:\n%s", tree)
	}
	if !strings.Contains(tree, "Enterprise B/") {
		t.Errorf("expected tree to contain 'Enterprise B/', got:\n%s", tree)
	}
	if !strings.Contains(tree, "assetid = 1") {
		t.Errorf("expected tree to contain 'assetid = 1', got:\n%s", tree)
	}
	if !strings.Contains(tree, "name = Site1") {
		t.Errorf("expected tree to contain 'name = Site1', got:\n%s", tree)
	}
}

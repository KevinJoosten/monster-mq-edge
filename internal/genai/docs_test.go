package genai

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDocsContext(t *testing.T) {
	tempDir := t.TempDir()

	docFile := filepath.Join(tempDir, "mqtt-guide.md")
	if err := os.WriteFile(docFile, []byte("# MQTT Guide\nContent here"), 0644); err != nil {
		t.Fatalf("failed to write test doc: %v", err)
	}

	// Normal load
	content, err := LoadDocsContext(tempDir, []string{"mqtt-guide"})
	if err != nil {
		t.Fatalf("expected doc to load, got error: %v", err)
	}
	if !strings.Contains(content, "# MQTT Guide") {
		t.Fatalf("expected content to contain doc content, got: %s", content)
	}

	// Path traversal attempt
	_, err = LoadDocsContext(tempDir, []string{"../../etc/passwd"})
	if err == nil {
		t.Fatalf("expected path traversal attempt to return error, got nil")
	}
}

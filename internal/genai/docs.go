package genai

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadDocsContext reads markdown files from docsPath for the given requested doc names.
// It verifies that each file stays strictly within the docsPath root to prevent path traversal attacks.
func LoadDocsContext(docsPath string, docNames []string) (string, error) {
	if len(docNames) == 0 {
		return "", nil
	}
	if docsPath == "" {
		docsPath = "docs"
	}

	cleanBase, err := filepath.Abs(filepath.Clean(docsPath))
	if err != nil {
		return "", fmt.Errorf("invalid docs path: %w", err)
	}

	var sb strings.Builder
	for _, name := range docNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		// Ensure extension
		if !strings.HasSuffix(strings.ToLower(name), ".md") {
			name += ".md"
		}

		targetPath, err := filepath.Abs(filepath.Join(cleanBase, name))
		if err != nil {
			continue
		}

		// Prevent directory traversal
		rel, err := filepath.Rel(cleanBase, targetPath)
		if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			return "", fmt.Errorf("access denied: documentation path outside configured directory: %s", name)
		}

		data, err := os.ReadFile(targetPath)
		if err != nil {
			// Skip missing docs
			continue
		}

		sb.WriteString(fmt.Sprintf("\n--- Documentation: %s ---\n", rel))
		sb.Write(data)
		sb.WriteString("\n")
	}

	return sb.String(), nil
}

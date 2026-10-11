package genai

import (
	"sort"
	"strings"
	"unicode/utf8"
)

type treeNode struct {
	name     string
	children map[string]*treeNode
	value    *string
}

func newTreeNode(name string) *treeNode {
	return &treeNode{
		name:     name,
		children: make(map[string]*treeNode),
	}
}

// FormatTopicValue converts a payload byte slice to an LLM context string.
// Returns (value, ok). If ok is false, the payload was binary, invalid UTF-8, or exceeded size limits.
func FormatTopicValue(payload []byte, maxLength int) (string, bool) {
	if len(payload) == 0 {
		return "", true
	}

	// Skip large payloads if maxLength > 0, or guard against extreme sizes (>5MB) if maxLength <= 0
	if maxLength > 0 && len(payload) > max(10*1024, maxLength*4) {
		return "", false
	}
	if maxLength <= 0 && len(payload) > 5*1024*1024 {
		return "", false
	}

	if !utf8.Valid(payload) {
		return "", false
	}

	stringValue := string(payload)

	// Skip if contains control characters other than newline, carriage return, or tab
	for _, r := range stringValue {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return "", false
		}
	}

	// Truncate if too long (0 or negative means unlimited)
	if maxLength > 0 && len(stringValue) > maxLength {
		return stringValue[:maxLength] + "...[truncated]", true
	}
	return stringValue, true
}

// FormatTopicsAsHierarchy builds an ASCII tree string from topic-value pairs.
func FormatTopicsAsHierarchy(topicValues map[string]string) string {
	if len(topicValues) == 0 {
		return ""
	}

	root := newTreeNode("")

	// Insert all topics into the tree
	for topic, value := range topicValues {
		parts := strings.Split(topic, "/")
		current := root

		for i, part := range parts {
			if part == "" {
				continue
			}

			isLeaf := i == len(parts)-1
			child, exists := current.children[part]
			if !exists {
				child = newTreeNode(part)
				current.children[part] = child
			}

			if isLeaf {
				valCopy := value
				child.value = &valCopy
			}
			current = child
		}
	}

	var sb strings.Builder
	renderNode(root, "", true, true, &sb)
	return sb.String()
}

func renderNode(node *treeNode, prefix string, isLast bool, isRoot bool, sb *strings.Builder) {
	// Sort children: directories first (nodes with children), then leaves
	children := make([]*treeNode, 0, len(node.children))
	for _, child := range node.children {
		children = append(children, child)
	}

	sort.Slice(children, func(i, j int) bool {
		iIsLeaf := len(children[i].children) == 0 && children[i].value != nil
		jIsLeaf := len(children[j].children) == 0 && children[j].value != nil
		if iIsLeaf != jIsLeaf {
			return !iIsLeaf // directories first
		}
		return children[i].name < children[j].name
	})

	for i, child := range children {
		isLastChild := i == len(children)-1
		hasChildren := len(child.children) > 0
		hasValue := child.value != nil

		connector := "├── "
		if isLastChild {
			connector = "└── "
		}

		childPrefix := prefix
		if isLastChild {
			childPrefix += "    "
		} else {
			childPrefix += "│   "
		}

		if hasChildren && !hasValue {
			sb.WriteString(prefix + connector + child.name + "/\n")
			renderNode(child, childPrefix, isLastChild, false, sb)
		} else if hasValue && !hasChildren {
			sb.WriteString(prefix + connector + child.name + " = " + *child.value + "\n")
		} else if hasValue && hasChildren {
			sb.WriteString(prefix + connector + child.name + " = " + *child.value + "\n")
			renderNode(child, childPrefix, isLastChild, false, sb)
		}
	}
}

package unsmodeler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/winccoanative"
)

// pathStep is one step of a jsonPath: a key or an array index.
type pathStep struct {
	key   string
	index int
	isIdx bool
}

// parseJSONPath parses the schema's jsonPath subset: $ followed by .key,
// ['key'], ["key"] or [index] steps.
func parseJSONPath(p string) ([]pathStep, error) {
	if !strings.HasPrefix(p, "$") || len(p) < 2 {
		return nil, fmt.Errorf("jsonPath %q must start with $ and have a step", p)
	}
	var out []pathStep
	s := p[1:]
	for s != "" {
		switch {
		case s[0] == '.':
			end := strings.IndexAny(s[1:], ".[")
			if end < 0 {
				end = len(s) - 1
			}
			key := s[1 : end+1]
			if key == "" {
				return nil, fmt.Errorf("jsonPath %q has an empty key", p)
			}
			out = append(out, pathStep{key: key})
			s = s[end+1:]
		case strings.HasPrefix(s, "['") || strings.HasPrefix(s, `["`):
			q := s[1]
			end := strings.Index(s[2:], string(q)+"]")
			if end < 0 {
				return nil, fmt.Errorf("jsonPath %q has an unterminated key", p)
			}
			out = append(out, pathStep{key: s[2 : 2+end]})
			s = s[2+end+2:]
		case s[0] == '[':
			end := strings.IndexByte(s, ']')
			if end < 0 {
				return nil, fmt.Errorf("jsonPath %q has an unterminated index", p)
			}
			n, err := strconv.Atoi(s[1:end])
			if err != nil || n < 0 {
				return nil, fmt.Errorf("jsonPath %q has an invalid index", p)
			}
			out = append(out, pathStep{index: n, isIdx: true})
			s = s[end+1:]
		default:
			return nil, fmt.Errorf("jsonPath %q: unexpected %q", p, s[:1])
		}
	}
	return out, nil
}

var errNoField = errors.New("field not found in the payload")

// extractJSONPath returns the raw JSON value at path in payload.
func extractJSONPath(payload []byte, path string) (json.RawMessage, error) {
	steps, err := parseJSONPath(path)
	if err != nil {
		return nil, err
	}
	cur := json.RawMessage(bytes.TrimSpace(payload))
	for _, st := range steps {
		if st.isIdx {
			var arr []json.RawMessage
			if json.Unmarshal(cur, &arr) != nil || st.index >= len(arr) {
				return nil, fmt.Errorf("%w: %s", errNoField, path)
			}
			cur = arr[st.index]
			continue
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(cur, &obj) != nil {
			return nil, fmt.Errorf("%w: %s (payload is not a JSON object)", errNoField, path)
		}
		v, ok := obj[st.key]
		if !ok {
			return nil, fmt.Errorf("%w: %s", errNoField, path)
		}
		cur = v
	}
	return cur, nil
}

// elementKind maps a Direction B element type to the value kind.
func elementKind(elementType string) oahost.Kind {
	switch elementType {
	case "bool":
		return oahost.KindBool
	case "int":
		return oahost.KindInt
	case "uint":
		return oahost.KindUint
	case "float":
		return oahost.KindFloat
	case "string":
		return oahost.KindString
	case "time":
		return oahost.KindTime
	case "bit32":
		return oahost.KindBit32
	case "blob":
		return oahost.KindBytes
	}
	return oahost.KindNull
}

// oaTypeOfElement is the WinCC OA element type name created for a
// Direction B element type.
func oaTypeOfElement(elementType string) string {
	switch elementType {
	case "bool", "int", "uint", "float", "string", "time", "bit32", "blob":
		return elementType
	}
	return ""
}

// convertPayload converts a received payload into the value of an element
// (§5): with jsonPath the field of a JSON payload; without, a string
// element takes text that is not a JSON string as is, a blob element the
// raw bytes, other types a bare JSON value. The native command form is not
// interpreted.
func convertPayload(payload []byte, jsonPath string, kind oahost.Kind) (oahost.Value, error) {
	if jsonPath != "" {
		raw, err := extractJSONPath(payload, jsonPath)
		if err != nil {
			return oahost.Value{}, err
		}
		return winccoanative.ConvertValue(raw, kind)
	}
	trimmed := bytes.TrimSpace(payload)
	switch kind {
	case oahost.KindBytes:
		return oahost.Value{Kind: oahost.KindBytes, Bytes: append([]byte(nil), payload...)}, nil
	case oahost.KindString:
		if len(trimmed) > 0 && trimmed[0] == '"' {
			var s string
			if json.Unmarshal(trimmed, &s) == nil {
				return oahost.Value{Kind: oahost.KindString, Str: s}, nil
			}
		}
		if !utf8.Valid(payload) {
			return oahost.Value{}, errors.New("payload is not UTF-8 text")
		}
		return oahost.Value{Kind: oahost.KindString, Str: string(payload)}, nil
	}
	return winccoanative.ConvertValue(trimmed, kind)
}

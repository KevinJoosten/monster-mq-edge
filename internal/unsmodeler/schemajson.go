package unsmodeler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// jsonObject is a decoded JSON object that keeps the key order of the
// document, so issues come out in document order.
type jsonObject struct {
	keys []string
	vals map[string]any
}

// errTooDeep stops decoding at the first value nested deeper than
// MaxJSONDepth; ptr is where it starts.
type errTooDeep struct{ ptr string }

func (e *errTooDeep) Error() string { return "too deep at " + e.ptr }

// docDecoder decodes a document into jsonObject, []any, string,
// json.Number, bool and nil. It reports duplicate keys (encoding/json keeps
// the last one silently) and bounds the nesting depth.
type docDecoder struct {
	dec    *json.Decoder
	issues []SchemaIssue
}

// decodeDocument returns the decoded document and the duplicate-key issues,
// or ok false and a single issue when the document is not usable JSON.
func decodeDocument(doc []byte) (v any, issues []SchemaIssue, ok bool) {
	d := &docDecoder{dec: json.NewDecoder(bytes.NewReader(doc))}
	d.dec.UseNumber()
	v, err := d.value("", 0)
	if err == nil {
		if _, terr := d.dec.Token(); terr != io.EOF {
			err = fmt.Errorf("unexpected data after the document")
		}
	}
	if err != nil {
		var deep *errTooDeep
		if errors.As(err, &deep) {
			return nil, []SchemaIssue{{Path: deep.ptr, Message: fmt.Sprintf("JSON nesting deeper than %d levels", MaxJSONDepth)}}, false
		}
		return nil, []SchemaIssue{{Path: "", Message: fmt.Sprintf("invalid JSON at byte %d: %v", d.dec.InputOffset(), err)}}, false
	}
	return v, d.issues, true
}

func (d *docDecoder) value(ptr string, depth int) (any, error) {
	tok, err := d.dec.Token()
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}
	if depth >= MaxJSONDepth {
		return nil, &errTooDeep{ptr: ptr}
	}
	switch delim {
	case '{':
		obj := &jsonObject{vals: map[string]any{}}
		for d.dec.More() {
			kt, err := d.dec.Token()
			if err != nil {
				return nil, err
			}
			key, _ := kt.(string)
			p := ptr + "/" + pointerEscape(key)
			v, err := d.value(p, depth+1)
			if err != nil {
				return nil, err
			}
			if _, dup := obj.vals[key]; dup {
				d.issues = append(d.issues, SchemaIssue{Path: p, Message: fmt.Sprintf("duplicate key %s; every key may appear only once per object", quoteJSON(key))})
			} else {
				obj.keys = append(obj.keys, key)
			}
			obj.vals[key] = v
		}
		if _, err := d.dec.Token(); err != nil {
			return nil, err
		}
		return obj, nil
	default: // '['
		arr := []any{}
		for d.dec.More() {
			v, err := d.value(ptr+"/"+strconv.Itoa(len(arr)), depth+1)
			if err != nil {
				return nil, err
			}
			arr = append(arr, v)
		}
		if _, err := d.dec.Token(); err != nil {
			return nil, err
		}
		return arr, nil
	}
}

// pointerEscape escapes one JSON Pointer reference token (RFC 6901).
func pointerEscape(s string) string {
	if !strings.ContainsAny(s, "~/") {
		return s
	}
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1")
}

// treeTooDeep returns the path of the first node deeper than MaxTreeDepth.
func treeTooDeep(doc any) (string, bool) {
	root, ok := doc.(*jsonObject)
	if !ok {
		return "", false
	}
	type item struct {
		nodes []any
		ptr   string
		depth int
	}
	tree, _ := root.vals["tree"].([]any)
	stack := []item{{tree, "/tree", 1}}
	for len(stack) > 0 {
		it := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for i, n := range it.nodes {
			p := it.ptr + "/" + strconv.Itoa(i)
			if it.depth > MaxTreeDepth {
				return p, true
			}
			if obj, ok := n.(*jsonObject); ok {
				if ch, ok := obj.vals["children"].([]any); ok {
					stack = append(stack, item{ch, p + "/children", it.depth + 1})
				}
			}
		}
	}
	return "", false
}

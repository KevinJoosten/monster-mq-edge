package unsmodeler

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strconv"
)

// NewNodeID returns a fresh random node id.
func NewNodeID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "n" + hex.EncodeToString(b[:])
}

// AssignNodeIDs gives every node of a Direction A tree without an id (or
// with an id that is not a string) a fresh unique id. It works on any
// well-formed document, also one that fails the schema, keeps the key order
// and returns the document unchanged when nothing was assigned. ok is false
// when the document is not well-formed JSON.
func AssignNodeIDs(raw []byte) (out []byte, assigned int, ok bool) {
	v, _, ok := decodeDocument(raw)
	if !ok {
		return raw, 0, false
	}
	root, isObj := v.(*jsonObject)
	if !isObj {
		return raw, 0, true
	}
	used := map[string]bool{}
	var collect func(nodes []any)
	collect = func(nodes []any) {
		for _, n := range nodes {
			o, ok := n.(*jsonObject)
			if !ok {
				continue
			}
			if id, ok := o.vals["id"].(string); ok {
				used[id] = true
			}
			if ch, ok := o.vals["children"].([]any); ok {
				collect(ch)
			}
		}
	}
	tree, _ := root.vals["tree"].([]any)
	collect(tree)
	var assign func(nodes []any)
	assign = func(nodes []any) {
		for _, n := range nodes {
			o, ok := n.(*jsonObject)
			if !ok {
				continue
			}
			if _, ok := o.vals["id"].(string); !ok {
				id := NewNodeID()
				for used[id] {
					id = NewNodeID()
				}
				used[id] = true
				if _, present := o.vals["id"]; !present {
					o.keys = append([]string{"id"}, o.keys...)
				}
				o.vals["id"] = id
				assigned++
			}
			if ch, ok := o.vals["children"].([]any); ok {
				assign(ch)
			}
		}
	}
	assign(tree)
	if assigned == 0 {
		return raw, 0, true
	}
	var b bytes.Buffer
	encodeOrdered(&b, root)
	return b.Bytes(), assigned, true
}

// encodeOrdered writes a decoded document back as compact JSON with the
// original key order and number spelling.
func encodeOrdered(b *bytes.Buffer, v any) {
	switch x := v.(type) {
	case *jsonObject:
		b.WriteByte('{')
		for i, k := range x.keys {
			if i > 0 {
				b.WriteByte(',')
			}
			kb, _ := marshalNoEscape(k)
			b.Write(kb)
			b.WriteByte(':')
			encodeOrdered(b, x.vals[k])
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			encodeOrdered(b, e)
		}
		b.WriteByte(']')
	case json.Number:
		b.WriteString(x.String())
	case string:
		sb, _ := marshalNoEscape(x)
		b.Write(sb)
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case nil:
		b.WriteString("null")
	}
}

// nodeIDs returns every node id of a typed document.
func nodeIDs(doc *Document) map[string]*Node {
	out := map[string]*Node{}
	doc.Walk(func(_ string, _ []*Node, n *Node) {
		if n.ID != "" {
			out[n.ID] = n
		}
	})
	return out
}

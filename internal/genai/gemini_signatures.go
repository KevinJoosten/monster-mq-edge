package genai

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"sync"
)

// signatureTransport keeps the extra_content (Gemini thought signatures) of
// the tool calls of OpenAI-compatible Gemini responses and puts it back on
// the same tool calls in later requests. Gemini 3 models reject a tool call
// without its signature; the langchaingo openai client drops the field.
type signatureTransport struct {
	base http.RoundTripper
	mu   sync.Mutex
	sigs map[string]json.RawMessage // tool call id -> extra_content
}

func newSignatureTransport() *signatureTransport {
	return &signatureTransport{base: http.DefaultTransport, sigs: map[string]json.RawMessage{}}
}

func (t *signatureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil && req.Method == http.MethodPost {
		body, err := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, err
		}
		body = t.inject(body)
		req = req.Clone(req.Context())
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusOK || resp.Body == nil {
		return resp, err
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	t.remember(body)
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return resp, nil
}

type sigToolCall struct {
	ID           string          `json:"id"`
	ExtraContent json.RawMessage `json:"extra_content"`
}

func (t *signatureTransport) remember(body []byte) {
	var r struct {
		Choices []struct {
			Message struct {
				ToolCalls []sigToolCall `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if json.Unmarshal(body, &r) != nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range r.Choices {
		for _, tc := range c.Message.ToolCalls {
			if tc.ID != "" && len(tc.ExtraContent) > 0 {
				t.sigs[tc.ID] = tc.ExtraContent
			}
		}
	}
}

func (t *signatureTransport) inject(body []byte) []byte {
	t.mu.Lock()
	n := len(t.sigs)
	t.mu.Unlock()
	if n == 0 {
		return body
	}
	var req map[string]json.RawMessage
	if json.Unmarshal(body, &req) != nil {
		return body
	}
	var msgs []map[string]json.RawMessage
	if json.Unmarshal(req["messages"], &msgs) != nil {
		return body
	}
	changed := false
	t.mu.Lock()
	for _, m := range msgs {
		var calls []map[string]json.RawMessage
		if json.Unmarshal(m["tool_calls"], &calls) != nil || len(calls) == 0 {
			continue
		}
		for _, c := range calls {
			var id string
			_ = json.Unmarshal(c["id"], &id)
			if sig, ok := t.sigs[id]; ok {
				c["extra_content"] = sig
				changed = true
			}
		}
		m["tool_calls"], _ = json.Marshal(calls)
	}
	t.mu.Unlock()
	if !changed {
		return body
	}
	req["messages"], _ = json.Marshal(msgs)
	out, err := json.Marshal(req)
	if err != nil {
		return body
	}
	return out
}

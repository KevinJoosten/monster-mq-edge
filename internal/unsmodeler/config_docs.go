package unsmodeler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"monstermq.io/edge/internal/stores"
)

// The draft and the applied mapping document are kept in the device's own
// configuration JSON, next to the settings, under these keys.
const (
	SlotDraft   = "draft"
	SlotApplied = "applied"
)

func configFields(config string) (map[string]json.RawMessage, error) {
	fields := map[string]json.RawMessage{}
	if strings.TrimSpace(config) == "" {
		return fields, nil
	}
	if err := json.Unmarshal([]byte(config), &fields); err != nil {
		return nil, fmt.Errorf("device configuration: %w", err)
	}
	return fields, nil
}

// encodeFields writes the fields in key order; values are written as
// stored, so a document keeps its bytes and its member order.
func encodeFields(fields map[string]json.RawMessage) string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		key, _ := marshalNoEscape(k)
		b.Write(bytes.TrimSpace(key))
		b.WriteByte(':')
		b.Write(fields[k])
	}
	b.WriteByte('}')
	return b.String()
}

// ConfigDocument returns the document of slot stored in a device
// configuration (nil when there is none).
func ConfigDocument(config, slot string) json.RawMessage {
	fields, err := configFields(config)
	if err != nil {
		return nil
	}
	doc := fields[slot]
	if len(doc) == 0 || string(doc) == "null" {
		return nil
	}
	return doc
}

// WithConfigDocument returns config with the document of slot replaced
// (removed when doc is nil).
func WithConfigDocument(config, slot string, doc json.RawMessage) (string, error) {
	fields, err := configFields(config)
	if err != nil {
		return "", err
	}
	if doc == nil {
		delete(fields, slot)
	} else {
		var c bytes.Buffer
		if err := json.Compact(&c, doc); err != nil {
			return "", err
		}
		fields[slot] = c.Bytes()
	}
	return encodeFields(fields), nil
}

// WithConfigDocuments returns settings (an encoded settings object) with
// the documents of from.
func WithConfigDocuments(settings, from string) (string, error) {
	fields, err := configFields(settings)
	if err != nil {
		return "", err
	}
	old, err := configFields(from)
	if err != nil {
		return "", err
	}
	for _, slot := range []string{SlotDraft, SlotApplied} {
		delete(fields, slot)
		if doc, ok := old[slot]; ok {
			fields[slot] = doc
		}
	}
	return encodeFields(fields), nil
}

// settingsKey is the configuration without the documents: a change of a
// document does not restart the device.
func settingsKey(config string) string {
	fields, err := configFields(config)
	if err != nil {
		return config
	}
	delete(fields, SlotDraft)
	delete(fields, SlotApplied)
	return encodeFields(fields)
}

func (m *Manager) getDoc(ctx context.Context, name, slot string) (json.RawMessage, error) {
	d, err := m.store.Get(ctx, name)
	if err != nil || d == nil {
		return nil, err
	}
	return ConfigDocument(d.Config, slot), nil
}

func (m *Manager) saveDoc(ctx context.Context, name, slot string, doc json.RawMessage) error {
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	d, err := m.store.Get(ctx, name)
	if err != nil {
		return err
	}
	if d == nil || d.Type != DeviceType {
		return fmt.Errorf("%w: %s", ErrNoDevice, name)
	}
	if d.Config, err = WithConfigDocument(d.Config, slot, doc); err != nil {
		return err
	}
	return m.store.Save(ctx, *d)
}

// SaveDevice stores a device configuration whose Config holds the
// settings; the stored documents of the device are kept.
func (m *Manager) SaveDevice(ctx context.Context, d stores.DeviceConfig) error {
	m.cfgMu.Lock()
	defer m.cfgMu.Unlock()
	cur, err := m.store.Get(ctx, d.Name)
	if err != nil {
		return err
	}
	from := ""
	if cur != nil && cur.Type == DeviceType {
		from = cur.Config
	}
	if d.Config, err = WithConfigDocuments(d.Config, from); err != nil {
		return err
	}
	return m.store.Save(ctx, d)
}

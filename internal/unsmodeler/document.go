package unsmodeler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// Document is the typed form of a mapping document. It is only built from a
// document without schema issues, so every field already has the shape the
// schema requires.
type Document struct {
	SchemaVersion int
	Direction     string
	// Direction A
	Tree []*Node
	// Direction B
	Source     *Source
	Types      []*TypeProposal
	Datapoints []*Datapoint
	AiRun      *AiRun
}

// Node is one UNS node of a Direction A tree.
type Node struct {
	ID          string     `json:"id,omitempty"`
	Name        string     `json:"name"`
	Level       string     `json:"level,omitempty"`
	Description string     `json:"description,omitempty"`
	AiGenerated bool       `json:"aiGenerated,omitempty"`
	Overrides   *Overrides `json:"overrides,omitempty"`
	Binding     *Binding   `json:"binding,omitempty"`
	Children    []*Node    `json:"children,omitempty"`
}

// Binding kinds.
const (
	KindElement   = "element"
	KindDatapoint = "datapoint"
	KindPattern   = "pattern"
)

// Binding binds WinCC OA data of one datapoint type to a node.
type Binding struct {
	Kind      string              `json:"kind"`
	Type      string              `json:"type"`
	System    string              `json:"system,omitempty"`
	Datapoint string              `json:"datapoint,omitempty"`
	Element   *string             `json:"element,omitempty"`
	Pattern   string              `json:"pattern,omitempty"`
	Elements  []*ElementSelection `json:"elements,omitempty"`
	Overrides *Overrides          `json:"overrides,omitempty"`
}

// ElementSelection is one selected element of a datapoint or pattern binding.
type ElementSelection struct {
	Path      string     `json:"path"`
	Topic     *string    `json:"topic,omitempty"`
	Overrides *Overrides `json:"overrides,omitempty"`
}

// Publish modes, timestamps and encodings.
const (
	ModeOnChange    = "onChange"
	ModeDeadband    = "deadband"
	ModeMinInterval = "minInterval"
	ModeCyclic      = "cyclic"

	TimestampSource  = "source"
	TimestampPublish = "publish"

	EncodingJSON = "json"
	EncodingRaw  = "raw"
)

// Overrides are topic semantics and payload fields of one level. The
// device setting topicDefaults has the same shape without Payload.
type Overrides struct {
	Retain      *bool          `json:"retain,omitempty"`
	Qos         *Int           `json:"qos,omitempty"`
	PublishMode *string        `json:"publishMode,omitempty"`
	Deadband    *float64       `json:"deadband,omitempty"`
	IntervalMs  *Int           `json:"intervalMs,omitempty"`
	Timestamp   *string        `json:"timestamp,omitempty"`
	Encoding    *string        `json:"encoding,omitempty"`
	Payload     []PayloadField `json:"payload,omitempty"`
}

// Predefined payload fields.
const (
	FieldValue       = "value"
	FieldTime        = "time"
	FieldUnit        = "unit"
	FieldDescription = "description"
	FieldFormat      = "format"
	FieldPvRange     = "pvRange"
	FieldStatus      = "status"
)

// PayloadField is a predefined field ("value", "time", ...) or a config
// attribute published under Name.
type PayloadField struct {
	Field     string // predefined field; empty for a config attribute
	Name      string
	Attribute string
}

func (p PayloadField) MarshalJSON() ([]byte, error) {
	if p.Field != "" {
		return json.Marshal(p.Field)
	}
	return json.Marshal(struct {
		Name      string `json:"name"`
		Attribute string `json:"attribute"`
	}{p.Name, p.Attribute})
}

func (p *PayloadField) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		return json.Unmarshal(b, &p.Field)
	}
	var o struct {
		Name      string `json:"name"`
		Attribute string `json:"attribute"`
	}
	if err := json.Unmarshal(b, &o); err != nil {
		return err
	}
	p.Name, p.Attribute = o.Name, o.Attribute
	return nil
}

// Key is the payload key of the field.
func (p PayloadField) Key() string {
	if p.Field != "" {
		return p.Field
	}
	return p.Name
}

// Int is an integer that also accepts integral numbers written as 1.0 or
// 1e3, as JSON Schema does.
type Int int64

func (i *Int) UnmarshalJSON(b []byte) error {
	f, err := strconv.ParseFloat(string(bytes.TrimSpace(b)), 64)
	if err != nil {
		return err
	}
	if f != math.Trunc(f) || math.Abs(f) > 1<<53 {
		return fmt.Errorf("not an integer: %s", b)
	}
	*i = Int(f)
	return nil
}

// Source is the analysed topic selection of a Direction B document.
type Source struct {
	Mode         string   `json:"mode"`
	ArchiveGroup string   `json:"archiveGroup,omitempty"`
	TopicFilters []string `json:"topicFilters"`
}

// TypeProposal is a datapoint type to create or to reuse.
type TypeProposal struct {
	Name     string         `json:"name"`
	Reuse    bool           `json:"reuse,omitempty"`
	Excluded bool           `json:"excluded,omitempty"`
	Elements []*TypeElement `json:"elements,omitempty"`
}

// TypeElement is a value element of a proposed type with its metadata.
type TypeElement struct {
	Path        string   `json:"path"`
	ElementType string   `json:"elementType"`
	Unit        *string  `json:"unit,omitempty"`
	Description *string  `json:"description,omitempty"`
	Format      *string  `json:"format,omitempty"`
	PvRange     *PvRange `json:"pvRange,omitempty"`
	Alert       *Alert   `json:"alert,omitempty"`
}

type PvRange struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

type Alert struct {
	Kind       string        `json:"kind"`
	Limits     []*AlertLimit `json:"limits,omitempty"`
	AlarmValue *bool         `json:"alarmValue,omitempty"`
	Class      string        `json:"class,omitempty"`
	Text       *string       `json:"text,omitempty"`
}

type AlertLimit struct {
	When  string  `json:"when"`
	Limit float64 `json:"limit"`
	Class string  `json:"class"`
	Text  *string `json:"text,omitempty"`
}

// Datapoint is a datapoint to create, fed from MQTT topics.
type Datapoint struct {
	Name     string              `json:"name"`
	Type     string              `json:"type"`
	Excluded bool                `json:"excluded,omitempty"`
	Elements []*DatapointElement `json:"elements,omitempty"`
	Bindings []*TopicBinding     `json:"bindings"`
}

// DatapointElement overrides the type-level metadata of one element.
type DatapointElement struct {
	Path        string   `json:"path"`
	Unit        *string  `json:"unit,omitempty"`
	Description *string  `json:"description,omitempty"`
	Format      *string  `json:"format,omitempty"`
	PvRange     *PvRange `json:"pvRange,omitempty"`
	Alert       *Alert   `json:"alert,omitempty"`
}

// TopicBinding writes the values of a topic to one element. With TimePath
// the value is written with dpSetTimed and the time read from the payload.
type TopicBinding struct {
	Element    string `json:"element"`
	Topic      string `json:"topic"`
	JSONPath   string `json:"jsonPath,omitempty"`
	TimePath   string `json:"timePath,omitempty"`
	TimeFormat string `json:"timeFormat,omitempty"`
}

// AiRun is the provenance of an AI-generated document.
type AiRun struct {
	Provider   string `json:"provider"`
	Model      string `json:"model"`
	FinishedAt string `json:"finishedAt,omitempty"`
}

type documentA struct {
	SchemaVersion Int     `json:"schemaVersion"`
	Direction     string  `json:"direction"`
	Tree          []*Node `json:"tree"`
	AiRun         *AiRun  `json:"aiRun,omitempty"`
}

type documentB struct {
	SchemaVersion Int             `json:"schemaVersion"`
	Direction     string          `json:"direction"`
	Source        *Source         `json:"source"`
	Types         []*TypeProposal `json:"types"`
	Datapoints    []*Datapoint    `json:"datapoints"`
	AiRun         *AiRun          `json:"aiRun,omitempty"`
}

// MarshalJSON writes the properties of the document's direction only.
func (d *Document) MarshalJSON() ([]byte, error) {
	if d.Direction == DirectionMqttToWinccoa {
		b := documentB{SchemaVersion: Int(d.SchemaVersion), Direction: d.Direction, Source: d.Source, Types: d.Types, Datapoints: d.Datapoints, AiRun: d.AiRun}
		if b.Types == nil {
			b.Types = []*TypeProposal{}
		}
		if b.Datapoints == nil {
			b.Datapoints = []*Datapoint{}
		}
		return marshalNoEscape(b)
	}
	a := documentA{SchemaVersion: Int(d.SchemaVersion), Direction: d.Direction, Tree: d.Tree, AiRun: d.AiRun}
	if a.Tree == nil {
		a.Tree = []*Node{}
	}
	return marshalNoEscape(a)
}

func (d *Document) UnmarshalJSON(b []byte) error {
	var raw struct {
		SchemaVersion Int             `json:"schemaVersion"`
		Direction     string          `json:"direction"`
		Tree          []*Node         `json:"tree"`
		Source        *Source         `json:"source"`
		Types         []*TypeProposal `json:"types"`
		Datapoints    []*Datapoint    `json:"datapoints"`
		AiRun         *AiRun          `json:"aiRun"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*d = Document{SchemaVersion: int(raw.SchemaVersion), Direction: raw.Direction, Tree: raw.Tree, Source: raw.Source, Types: raw.Types, Datapoints: raw.Datapoints, AiRun: raw.AiRun}
	return nil
}

// ParseDocument decodes a structurally valid document (no ValidateSchema
// issues) into its typed form.
func ParseDocument(doc []byte) (*Document, error) {
	var d Document
	if err := json.Unmarshal(doc, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// Encode serializes the document without HTML escaping.
func (d *Document) Encode() ([]byte, error) { return d.MarshalJSON() }

func marshalNoEscape(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// Walk calls fn for every node of the tree depth-first with its JSON
// Pointer, the names of its ancestors and the node itself.
func (d *Document) Walk(fn func(ptr string, ancestors []*Node, n *Node)) {
	var walk func(nodes []*Node, ptr string, anc []*Node)
	walk = func(nodes []*Node, ptr string, anc []*Node) {
		for i, n := range nodes {
			p := ptr + "/" + strconv.Itoa(i)
			fn(p, anc, n)
			walk(n.Children, p+"/children", append(anc[:len(anc):len(anc)], n))
		}
	}
	walk(d.Tree, "/tree", nil)
}

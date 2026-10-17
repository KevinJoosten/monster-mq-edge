package resolvers

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"monstermq.io/edge/internal/bridge/kafkaclient"
	"monstermq.io/edge/internal/graphql/generated"
	"monstermq.io/edge/internal/stores"
)

// Kafka client bridge (KAFKA-Client), ported from Core's
// KafkaClientConfigQueries.kt / KafkaClientConfigMutations.kt.

const kafkaFeatureDisabled = "Kafka feature is not enabled on this node"

var deviceNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func kafkaDisabledResult() *generated.KafkaClientResult {
	return &generated.KafkaClientResult{Success: false, Errors: []string{kafkaFeatureDisabled}}
}

func (r *Resolver) KafkaClient() generated.KafkaClientResolver { return &kafkaClientResolver{r} }
func (r *Resolver) KafkaClientMutations() generated.KafkaClientMutationsResolver {
	return &kafkaClientMutationsResolver{r}
}

type kafkaClientResolver struct{ *Resolver }
type kafkaClientMutationsResolver struct{ *Resolver }

func (r *mutationResolver) KafkaClient(ctx context.Context) (*generated.KafkaClientMutations, error) {
	return &generated.KafkaClientMutations{}, nil
}

// Query: kafkaClients(name, node) -------------------------------------------

func (r *queryResolver) KafkaClients(ctx context.Context, name, node *string) ([]*generated.KafkaClient, error) {
	out := []*generated.KafkaClient{}
	if !r.Cfg.Features.Kafka {
		return out, nil
	}
	devices, err := r.Storage.DeviceConfig.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	for _, d := range devices {
		if d.Type != kafkaclient.DeviceTypeKafkaClient {
			continue
		}
		if name != nil && d.Name != *name {
			continue
		}
		// As Core's getDevicesByNode, a node filter also matches devices for every node.
		if node != nil && d.NodeID != *node && d.NodeID != "*" {
			continue
		}
		out = append(out, r.deviceToKafkaClient(d))
	}
	return out, nil
}

func (r *Resolver) deviceToKafkaClient(d stores.DeviceConfig) *generated.KafkaClient {
	cfg, err := kafkaclient.ParseConfig(d.Config)
	if err != nil {
		// As Core: show defaults so the dashboard can still list the device.
		r.Logger.Warn("kafka client config parse failed", "name", d.Name, "err", err)
		cfg = kafkaclient.DefaultConfig()
	}
	filters := cfg.OutboundTopicFilters
	if filters == nil {
		filters = []string{}
	}
	return &generated.KafkaClient{
		Name:      d.Name,
		Namespace: d.Namespace,
		NodeID:    d.NodeID,
		Enabled:   d.Enabled,
		Config: &generated.KafkaClientConfig{
			BootstrapServers:            cfg.BootstrapServers,
			GroupID:                     cfg.GroupID,
			PayloadFormat:               generated.KafkaPayloadFormat(cfg.PayloadFormat),
			ExtraConsumerConfig:         stringMapToAny(cfg.ExtraConsumerConfig),
			ReconnectDelayMs:            cfg.ReconnectDelayMs,
			DestinationTopicPrefix:      cfg.DestinationTopicPrefix,
			TopicKeyRegex:               cfg.TopicKeyRegex,
			TopicKeyReplacement:         cfg.TopicKeyReplacement,
			InboundEnabled:              cfg.InboundEnabled,
			OutboundEnabled:             cfg.OutboundEnabled,
			OutboundTopicFilters:        filters,
			OutboundKafkaTopic:          cfg.OutboundKafkaTopic,
			ExtraProducerConfig:         stringMapToAny(cfg.ExtraProducerConfig),
			OutboundTopicKeyRegex:       cfg.OutboundTopicKeyRegex,
			OutboundTopicKeyReplacement: cfg.OutboundTopicKeyReplacement,
		},
		CreatedAt:       formatTime(d.CreatedAt),
		UpdatedAt:       formatTime(d.UpdatedAt),
		IsOnCurrentNode: d.NodeID == r.NodeID || d.NodeID == "local" || d.NodeID == "*",
	}
}

func stringMapToAny(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Field resolvers on KafkaClient ---------------------------------------------

// Metrics is Core's kafkaClientMetrics: the newest stored sample of the last
// minute, else the running connector's latest sample, else zeros; rates are
// rounded to two decimals.
func (r *kafkaClientResolver) Metrics(ctx context.Context, obj *generated.KafkaClient) ([]*generated.KafkaClientMetrics, error) {
	zero := []*generated.KafkaClientMetrics{{Timestamp: nowISO()}}
	if obj == nil {
		return zero, nil
	}
	if r.Storage.Metrics != nil {
		now := time.Now()
		if rows, err := r.kafkaMetricRows(ctx, obj.Name, now.Add(-time.Minute), now); err == nil && len(rows) > 0 {
			return rows[:1], nil
		}
	}
	if r.KafkaClients != nil {
		if c := r.KafkaClients.Get(obj.Name); c != nil {
			return []*generated.KafkaClientMetrics{kafkaSnapshotToMetrics(c.LatestMetrics())}, nil
		}
	}
	return zero, nil
}

// MetricsHistory is Core's kafkaClientMetricsHistory: lastMinutes, else from
// (with an optional to) selects the range; without either there is none and
// the result is empty. Newest first, no limit.
func (r *kafkaClientResolver) MetricsHistory(ctx context.Context, obj *generated.KafkaClient, from, to *string, lastMinutes *int) ([]*generated.KafkaClientMetrics, error) {
	if r.Storage.Metrics == nil || obj == nil {
		return []*generated.KafkaClientMetrics{}, nil
	}
	start, err := parseTimeArg(from)
	if err != nil {
		return nil, err
	}
	end, err := parseTimeArg(to)
	if err != nil {
		return nil, err
	}
	switch {
	case lastMinutes != nil:
		now := time.Now()
		t := now.Add(-time.Duration(*lastMinutes) * time.Minute)
		start, end = &t, &now
	case start == nil:
		return []*generated.KafkaClientMetrics{}, nil
	case end == nil:
		far := time.Now().AddDate(100, 0, 0)
		end = &far
	}
	rows, err := r.kafkaMetricRows(ctx, obj.Name, *start, *end)
	if err != nil {
		r.Logger.Warn("kafka client metrics history failed", "name", obj.Name, "err", err)
		return []*generated.KafkaClientMetrics{}, nil
	}
	return rows, nil
}

// kafkaMetricRows reads stored samples, newest first, as Core's store does.
func (r *Resolver) kafkaMetricRows(ctx context.Context, name string, from, to time.Time) ([]*generated.KafkaClientMetrics, error) {
	rows, err := r.Storage.Metrics.GetHistory(ctx, stores.MetricKafkaClient, name, from, to, math.MaxInt32)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Timestamp.After(rows[j].Timestamp) })
	out := make([]*generated.KafkaClientMetrics, 0, len(rows))
	for _, row := range rows {
		var snap kafkaclient.MetricsSnapshot
		_ = json.Unmarshal([]byte(row.Payload), &snap)
		if snap.Timestamp.IsZero() {
			snap.Timestamp = row.Timestamp
		}
		out = append(out, kafkaSnapshotToMetrics(snap))
	}
	return out, nil
}

func kafkaSnapshotToMetrics(s kafkaclient.MetricsSnapshot) *generated.KafkaClientMetrics {
	ts := s.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return &generated.KafkaClientMetrics{MessagesIn: round2(s.MessagesIn), MessagesOut: round2(s.MessagesOut), Timestamp: formatTime(ts)}
}

// round2 is Core's rate rounding; Kotlin's round() rounds ties to even, as
// math.RoundToEven does.
func round2(v float64) float64 { return math.RoundToEven(v*100) / 100 }

// Mutations group: kafkaClient ------------------------------------------------

// kafkaInputToConfig mirrors Core's parseDeviceConfigRequest: the prefix is
// stored as entered (blank -> null) and normalised whenever it is read.
func kafkaInputToConfig(in *generated.KafkaClientConfigInput) kafkaclient.Config {
	cfg := kafkaclient.DefaultConfig()
	if in == nil {
		return cfg
	}
	if in.BootstrapServers != nil {
		cfg.BootstrapServers = *in.BootstrapServers
	}
	if in.GroupID != nil {
		cfg.GroupID = *in.GroupID
	}
	if in.PayloadFormat != nil {
		cfg.PayloadFormat = string(*in.PayloadFormat)
	}
	if in.ExtraConsumerConfig != nil {
		cfg.ExtraConsumerConfig = anyMapToString(in.ExtraConsumerConfig)
	}
	if in.ReconnectDelayMs != nil {
		cfg.ReconnectDelayMs = *in.ReconnectDelayMs
	}
	cfg.DestinationTopicPrefix = nonBlankPtr(in.DestinationTopicPrefix)
	cfg.TopicKeyRegex = nonBlankPtr(in.TopicKeyRegex)
	cfg.TopicKeyReplacement = in.TopicKeyReplacement
	if in.InboundEnabled != nil {
		cfg.InboundEnabled = *in.InboundEnabled
	}
	if in.OutboundEnabled != nil {
		cfg.OutboundEnabled = *in.OutboundEnabled
	}
	if in.OutboundTopicFilters != nil {
		cfg.OutboundTopicFilters = in.OutboundTopicFilters
	}
	cfg.OutboundKafkaTopic = nonBlankPtr(in.OutboundKafkaTopic)
	if in.ExtraProducerConfig != nil {
		cfg.ExtraProducerConfig = anyMapToString(in.ExtraProducerConfig)
	}
	cfg.OutboundTopicKeyRegex = nonBlankPtr(in.OutboundTopicKeyRegex)
	cfg.OutboundTopicKeyReplacement = in.OutboundTopicKeyReplacement
	return cfg
}

func nonBlankPtr(s *string) *string {
	if s == nil || strings.TrimSpace(*s) == "" {
		return nil
	}
	return s
}

// validateKafkaInput returns the stored JSON or the validation errors. Core's
// checks come first; the stricter edge checks follow (plan E5).
func validateKafkaInput(in generated.KafkaClientInput) (string, []string) {
	var errs []string
	// Core's DeviceConfigRequest.validate().
	if strings.TrimSpace(in.Name) == "" {
		errs = append(errs, "name cannot be blank")
	}
	if !deviceNamePattern.MatchString(in.Name) {
		errs = append(errs, "name can only contain letters, numbers, underscores, and hyphens")
	}
	if strings.TrimSpace(in.Namespace) == "" {
		errs = append(errs, "namespace cannot be blank")
	}
	if strings.TrimSpace(in.NodeID) == "" {
		errs = append(errs, "nodeId cannot be blank")
	}
	raw, _ := json.Marshal(kafkaInputToConfig(in.Config))
	parsed, err := kafkaclient.ParseConfig(string(raw))
	if err != nil {
		return "", append(errs, "Invalid Kafka config: "+err.Error())
	}
	errs = append(errs, parsed.ValidateStrict()...)
	return string(raw), errs
}

func (r *kafkaClientMutationsResolver) Create(ctx context.Context, _ *generated.KafkaClientMutations, input generated.KafkaClientInput) (*generated.KafkaClientResult, error) {
	if !r.Cfg.Features.Kafka {
		return kafkaDisabledResult(), nil
	}
	raw, errs := validateKafkaInput(input)
	if len(errs) > 0 {
		return &generated.KafkaClientResult{Success: false, Errors: errs}, nil
	}
	if existing, _ := r.Storage.DeviceConfig.Get(ctx, input.Name); existing != nil {
		return &generated.KafkaClientResult{Success: false, Errors: []string{fmt.Sprintf("Device with name '%s' already exists", input.Name)}}, nil
	}
	d := stores.DeviceConfig{
		Name:      input.Name,
		Namespace: input.Namespace,
		NodeID:    input.NodeID,
		Type:      kafkaclient.DeviceTypeKafkaClient,
		Enabled:   boolPtr(input.Enabled, true),
		Config:    raw,
	}
	if err := r.Storage.DeviceConfig.Save(ctx, d); err != nil {
		return &generated.KafkaClientResult{Success: false, Errors: []string{"Failed to save device: " + err.Error()}}, nil
	}
	return r.kafkaResult(ctx, d, "create"), nil
}

func (r *kafkaClientMutationsResolver) Update(ctx context.Context, _ *generated.KafkaClientMutations, name string, input generated.KafkaClientInput) (*generated.KafkaClientResult, error) {
	if !r.Cfg.Features.Kafka {
		return kafkaDisabledResult(), nil
	}
	if name != input.Name {
		return &generated.KafkaClientResult{Success: false, Errors: []string{"name in path must match name in input"}}, nil
	}
	existing, err := r.Storage.DeviceConfig.Get(ctx, name)
	if err != nil || existing == nil {
		return &generated.KafkaClientResult{Success: false, Errors: []string{fmt.Sprintf("Device '%s' not found", name)}}, nil
	}
	raw, errs := validateKafkaInput(input)
	if len(errs) > 0 {
		return &generated.KafkaClientResult{Success: false, Errors: errs}, nil
	}
	// As Core, update stores the normalised config (create stores the input).
	if parsed, err := kafkaclient.ParseConfig(raw); err == nil {
		if b, err := json.Marshal(parsed); err == nil {
			raw = string(b)
		}
	}
	d := stores.DeviceConfig{
		Name:      input.Name,
		Namespace: input.Namespace,
		NodeID:    input.NodeID,
		Type:      kafkaclient.DeviceTypeKafkaClient,
		Enabled:   boolPtr(input.Enabled, true),
		Config:    raw,
		CreatedAt: existing.CreatedAt,
	}
	if err := r.Storage.DeviceConfig.Save(ctx, d); err != nil {
		return &generated.KafkaClientResult{Success: false, Errors: []string{"Failed to update device: " + err.Error()}}, nil
	}
	return r.kafkaResult(ctx, d, "update"), nil
}

func (r *kafkaClientMutationsResolver) kafkaResult(ctx context.Context, d stores.DeviceConfig, reason string) *generated.KafkaClientResult {
	saved, _ := r.Storage.DeviceConfig.Get(ctx, d.Name)
	if saved == nil {
		saved = &d
	}
	r.reloadKafka(ctx, reason)
	return &generated.KafkaClientResult{Success: true, Errors: []string{}, Client: r.deviceToKafkaClient(*saved)}
}

func (r *kafkaClientMutationsResolver) Delete(ctx context.Context, _ *generated.KafkaClientMutations, name string) (bool, error) {
	if !r.Cfg.Features.Kafka {
		return false, nil
	}
	existing, err := r.Storage.DeviceConfig.Get(ctx, name)
	// As Core, any device of that name is deleted, whatever its type.
	if err != nil || existing == nil {
		return false, nil
	}
	if err := r.Storage.DeviceConfig.Delete(ctx, name); err != nil {
		return false, nil
	}
	r.reloadKafka(ctx, "delete")
	return true, nil
}

func (r *kafkaClientMutationsResolver) Start(ctx context.Context, _ *generated.KafkaClientMutations, name string) (*generated.KafkaClientResult, error) {
	return r.toggle(ctx, name, true)
}

func (r *kafkaClientMutationsResolver) Stop(ctx context.Context, _ *generated.KafkaClientMutations, name string) (*generated.KafkaClientResult, error) {
	return r.toggle(ctx, name, false)
}

func (r *kafkaClientMutationsResolver) Toggle(ctx context.Context, _ *generated.KafkaClientMutations, name string, enabled bool) (*generated.KafkaClientResult, error) {
	return r.toggle(ctx, name, enabled)
}

func (r *kafkaClientMutationsResolver) toggle(ctx context.Context, name string, enabled bool) (*generated.KafkaClientResult, error) {
	if !r.Cfg.Features.Kafka {
		return kafkaDisabledResult(), nil
	}
	d, err := r.Storage.DeviceConfig.Toggle(ctx, name, enabled)
	if err != nil {
		return &generated.KafkaClientResult{Success: false, Errors: []string{"Failed to toggle device: " + err.Error()}}, nil
	}
	if d == nil {
		return &generated.KafkaClientResult{Success: false, Errors: []string{fmt.Sprintf("Device '%s' not found", name)}}, nil
	}
	r.reloadKafka(ctx, "toggle")
	return &generated.KafkaClientResult{Success: true, Errors: []string{}, Client: r.deviceToKafkaClient(*d)}, nil
}

func (r *kafkaClientMutationsResolver) Reassign(ctx context.Context, _ *generated.KafkaClientMutations, name, nodeID string) (*generated.KafkaClientResult, error) {
	if !r.Cfg.Features.Kafka {
		return kafkaDisabledResult(), nil
	}
	if nodes := r.clusterNodeIDs(); !slices.Contains(nodes, nodeID) {
		return &generated.KafkaClientResult{Success: false, Errors: []string{
			fmt.Sprintf("Cluster node '%s' not found. Available nodes: %s", nodeID, strings.Join(nodes, ", ")),
		}}, nil
	}
	d, err := r.Storage.DeviceConfig.Reassign(ctx, name, nodeID)
	if err != nil {
		return &generated.KafkaClientResult{Success: false, Errors: []string{"Failed to reassign device: " + err.Error()}}, nil
	}
	if d == nil {
		return &generated.KafkaClientResult{Success: false, Errors: []string{fmt.Sprintf("Device '%s' not found", name)}}, nil
	}
	r.reloadKafka(ctx, "reassign")
	return &generated.KafkaClientResult{Success: true, Errors: []string{}, Client: r.deviceToKafkaClient(*d)}, nil
}

// reloadKafka reconciles the Kafka manager with the stored devices.
// Best-effort: the config was already saved.
func (r *Resolver) reloadKafka(ctx context.Context, reason string) {
	if r.KafkaClients == nil {
		return
	}
	if err := r.KafkaClients.Reload(ctx); err != nil {
		r.Logger.Warn("kafka client reload after "+reason+" failed", "err", err)
	}
}

// clusterNodeIDs are the nodes a device can be assigned to, Core's cluster
// node list: this node and its PeerLink peers.
func (r *Resolver) clusterNodeIDs() []string {
	nodes := []string{r.NodeID}
	if r.PeerLinkMgr != nil {
		for _, p := range r.PeerLinkMgr.Peers() {
			if p.NodeID != "" && !slices.Contains(nodes, p.NodeID) {
				nodes = append(nodes, p.NodeID)
			}
		}
	}
	return nodes
}

// anyMapToString converts GraphQL JSON values the way Core's value.toString()
// does for the extra Kafka properties.
func anyMapToString(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = coreValueString(v)
	}
	return out
}

func coreValueString(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return strconv.FormatInt(i, 10)
		}
		f, _ := x.Float64()
		return kafkaclient.DoubleString(f)
	case int:
		return strconv.Itoa(x)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case float32:
		return kafkaclient.DoubleString(float64(x))
	case float64:
		return kafkaclient.DoubleString(x)
	case map[string]any:
		parts := make([]string, 0, len(x))
		for _, k := range slices.Sorted(maps.Keys(x)) {
			parts = append(parts, k+"="+coreValueString(x[k]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case []any:
		parts := make([]string, 0, len(x))
		for _, e := range x {
			parts = append(parts, coreValueString(e))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return fmt.Sprint(v)
}

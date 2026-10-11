package resolvers

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"monstermq.io/edge/internal/genai"
	"monstermq.io/edge/internal/graphql/generated"
	"monstermq.io/edge/internal/stores"
)

const deviceTypeGenAiProvider = "genai_provider"

var providerNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type genAiQueryResolver struct{ *Resolver }
type genAiProviderMutationsResolver struct{ *Resolver }

func (r *Resolver) GenAiQuery() generated.GenAiQueryResolver {
	return &genAiQueryResolver{r}
}

func (r *Resolver) GenAiProviderMutations() generated.GenAiProviderMutationsResolver {
	return &genAiProviderMutationsResolver{r}
}

func (r *mutationResolver) GenAiProvider(ctx context.Context) (*generated.GenAiProviderMutations, error) {
	return &generated.GenAiProviderMutations{}, nil
}

// Query: genai ---------------------------------------------------------------

func (r *queryResolver) Genai(ctx context.Context) (*generated.GenAiQuery, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		return nil, nil
	}
	return &generated.GenAiQuery{}, nil
}

// Query: genAiProviders ------------------------------------------------------

func (r *queryResolver) GenAiProviders(ctx context.Context) ([]*generated.GenAiProvider, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		return []*generated.GenAiProvider{}, nil
	}

	configProviders := r.getConfigProviders()

	var dbProviders []*generated.GenAiProvider
	if r.Storage != nil && r.Storage.DeviceConfig != nil {
		devices, err := r.Storage.DeviceConfig.GetByType(ctx, deviceTypeGenAiProvider)
		if err == nil {
			for _, d := range devices {
				dbProviders = append(dbProviders, deviceToGenAiProvider(d))
			}
		}
	}

	return append(configProviders, dbProviders...), nil
}

// Query: genAiProvider(name) -------------------------------------------------

func (r *queryResolver) GenAiProvider(ctx context.Context, name string) (*generated.GenAiProvider, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		return nil, nil
	}

	// First check database providers
	if r.Storage != nil && r.Storage.DeviceConfig != nil {
		d, err := r.Storage.DeviceConfig.Get(ctx, name)
		if err == nil && d != nil && d.Type == deviceTypeGenAiProvider {
			return deviceToGenAiProvider(*d), nil
		}
	}

	// Fallback to config providers
	for _, p := range r.getConfigProviders() {
		if p.Name == name {
			return p, nil
		}
	}

	return nil, nil
}

// Mutation: create -----------------------------------------------------------

func (r *genAiProviderMutationsResolver) Create(ctx context.Context, _ *generated.GenAiProviderMutations, name string, input generated.GenAiProviderInput) (*generated.GenAiProvider, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		return nil, fmt.Errorf("GenAi feature is not enabled on this node")
	}

	name = strings.TrimSpace(name)
	if !providerNameRegex.MatchString(name) {
		return nil, fmt.Errorf("invalid provider name '%s': only letters, digits, underscores, and hyphens are allowed", name)
	}

	if r.Storage == nil || r.Storage.DeviceConfig == nil {
		return nil, fmt.Errorf("DeviceConfig store is not available")
	}

	existing, err := r.Storage.DeviceConfig.Get(ctx, name)
	if err == nil && existing != nil {
		return nil, fmt.Errorf("GenAI provider '%s' already exists", name)
	}

	cfgJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to encode provider config: %w", err)
	}

	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}

	now := time.Now()
	dev := stores.DeviceConfig{
		Name:      name,
		Namespace: "genai",
		NodeID:    "*",
		Type:      deviceTypeGenAiProvider,
		Enabled:   enabled,
		Config:    string(cfgJSON),
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := r.Storage.DeviceConfig.Save(ctx, dev); err != nil {
		return nil, fmt.Errorf("failed to save GenAI provider: %w", err)
	}

	return deviceToGenAiProvider(dev), nil
}

// Mutation: update -----------------------------------------------------------

func (r *genAiProviderMutationsResolver) Update(ctx context.Context, _ *generated.GenAiProviderMutations, name string, input generated.GenAiProviderInput) (*generated.GenAiProvider, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		return nil, fmt.Errorf("GenAi feature is not enabled on this node")
	}

	if r.Storage == nil || r.Storage.DeviceConfig == nil {
		return nil, fmt.Errorf("DeviceConfig store is not available")
	}

	existing, err := r.Storage.DeviceConfig.Get(ctx, name)
	if err != nil || existing == nil || existing.Type != deviceTypeGenAiProvider {
		return nil, fmt.Errorf("GenAI provider not found: %s", name)
	}

	var existingCfg generated.GenAiProviderInput
	_ = json.Unmarshal([]byte(existing.Config), &existingCfg)

	// Merge updates
	if input.Type != "" {
		existingCfg.Type = input.Type
	}
	if input.Model != nil {
		existingCfg.Model = input.Model
	}
	// Preserve existing API key if masked or omitted
	if input.APIKey != nil && *input.APIKey != "" && *input.APIKey != "***" {
		existingCfg.APIKey = input.APIKey
	}
	if input.Endpoint != nil {
		existingCfg.Endpoint = input.Endpoint
	}
	if input.ServiceVersion != nil {
		existingCfg.ServiceVersion = input.ServiceVersion
	}
	if input.BaseURL != nil {
		existingCfg.BaseURL = input.BaseURL
	}
	if input.Temperature != nil {
		existingCfg.Temperature = input.Temperature
	}
	if input.MaxTokens != nil {
		existingCfg.MaxTokens = input.MaxTokens
	}

	enabled := existing.Enabled
	if input.Enabled != nil {
		enabled = *input.Enabled
		existingCfg.Enabled = input.Enabled
	}

	cfgJSON, err := json.Marshal(existingCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode provider config: %w", err)
	}

	updated := *existing
	updated.Config = string(cfgJSON)
	updated.Enabled = enabled
	updated.UpdatedAt = time.Now()

	if err := r.Storage.DeviceConfig.Save(ctx, updated); err != nil {
		return nil, fmt.Errorf("failed to update GenAI provider: %w", err)
	}

	return deviceToGenAiProvider(updated), nil
}

// Mutation: delete -----------------------------------------------------------

func (r *genAiProviderMutationsResolver) Delete(ctx context.Context, _ *generated.GenAiProviderMutations, name string) (bool, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		return false, nil
	}

	if r.Storage == nil || r.Storage.DeviceConfig == nil {
		return false, fmt.Errorf("DeviceConfig store is not available")
	}

	existing, err := r.Storage.DeviceConfig.Get(ctx, name)
	if err != nil || existing == nil || existing.Type != deviceTypeGenAiProvider {
		return false, nil
	}

	if err := r.Storage.DeviceConfig.Delete(ctx, name); err != nil {
		return false, err
	}
	return true, nil
}

// GenAiQuery: generate -------------------------------------------------------

func (r *genAiQueryResolver) Generate(ctx context.Context, _ *generated.GenAiQuery, prompt string, contextVal *string, docs []string) (*generated.GenAiResponse, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		errMsg := "GenAI is not enabled or not configured"
		return &generated.GenAiResponse{Response: "", Error: &errMsg}, nil
	}

	if strings.TrimSpace(prompt) == "" {
		errMsg := "Prompt is required and cannot be empty"
		modelName := r.Cfg.GenAI.Assistant.Model
		return &generated.GenAiResponse{Response: "", Model: &modelName, Error: &errMsg}, nil
	}

	provider, err := r.getGenAiProvider(ctx)
	if err != nil || provider == nil {
		errMsg := "GenAI provider is not available"
		if err != nil {
			errMsg = fmt.Sprintf("Failed to initialize GenAI provider: %s", err.Error())
		}
		return &generated.GenAiResponse{Response: "", Error: &errMsg}, nil
	}

	var ctxStr string
	if contextVal != nil {
		ctxStr = *contextVal
	}

	req := genai.Request{
		Prompt:  prompt,
		Context: ctxStr,
		Docs:    docs,
	}

	resp, err := provider.Generate(ctx, req)
	if err != nil {
		errMsg := fmt.Sprintf("AI request failed: %s", err.Error())
		modelName := provider.ModelName()
		return &generated.GenAiResponse{Response: "", Model: &modelName, Error: &errMsg}, nil
	}

	var modelPtr *string
	if resp.Model != "" {
		modelPtr = &resp.Model
	}
	var errPtr *string
	if resp.Error != "" {
		errPtr = &resp.Error
	}

	return &generated.GenAiResponse{
		Response: resp.Response,
		Model:    modelPtr,
		Error:    errPtr,
	}, nil
}

// GenAiQuery: analyzeTopics --------------------------------------------------

func (r *genAiQueryResolver) AnalyzeTopics(
	ctx context.Context,
	_ *generated.GenAiQuery,
	archiveGroup *string,
	topicPattern *string,
	topicData []*generated.TopicDataInput,
	question string,
	systemPrompt *string,
	maxTopics *int,
	maxValueLength *int,
	chatHistory []*generated.ChatMessage,
) (*generated.TopicAnalysisResponse, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		errMsg := "GenAI is not enabled or not configured"
		return &generated.TopicAnalysisResponse{Response: "", TopicsAnalyzed: 0, Error: &errMsg}, nil
	}

	provider, err := r.getGenAiProvider(ctx)
	if err != nil || provider == nil {
		errMsg := "GenAI provider is not available"
		if err != nil {
			errMsg = fmt.Sprintf("Failed to initialize GenAI provider: %s", err.Error())
		}
		return &generated.TopicAnalysisResponse{Response: "", TopicsAnalyzed: 0, Error: &errMsg}, nil
	}

	modelName := provider.ModelName()

	if strings.TrimSpace(question) == "" {
		errMsg := "Question is required and cannot be empty"
		return &generated.TopicAnalysisResponse{Response: "", TopicsAnalyzed: 0, Model: &modelName, Error: &errMsg}, nil
	}

	maxT := 100
	if maxTopics != nil {
		maxT = *maxTopics
	}

	maxLen := 1000
	if maxValueLength != nil {
		maxLen = *maxValueLength
	}

	topicValues := make(map[string]string)

	// 1. Gather from LastValueStore via topicPattern
	hasPattern := topicPattern != nil && strings.TrimSpace(*topicPattern) != ""
	if hasPattern {
		agName := "Default"
		if archiveGroup != nil && *archiveGroup != "" {
			agName = *archiveGroup
		}

		store := r.lastValueStore(&agName)
		if store != nil {
			pattern := strings.TrimSpace(*topicPattern)
			_ = store.FindMatchingMessages(ctx, pattern, func(m stores.BrokerMessage) bool {
				if maxT > 0 && len(topicValues) >= maxT {
					return false
				}
				val, ok := genai.FormatTopicValue(m.Payload, maxLen)
				if ok {
					topicValues[m.TopicName] = val
				}
				return true
			})
		}
	}

	// 2. Gather from direct topicData
	if len(topicData) > 0 {
		for _, td := range topicData {
			if td == nil {
				continue
			}
			top := strings.TrimSpace(td.Topic)
			if top == "" {
				continue
			}
			if maxT > 0 && len(topicValues) >= maxT && topicValues[top] == "" {
				break
			}
			rawVal := ""
			if td.Value != nil {
				rawVal = *td.Value
			}
			if maxLen > 0 && len(rawVal) > maxLen {
				rawVal = rawVal[:maxLen] + "...[truncated]"
			}
			topicValues[top] = rawVal
		}
	}

	isFollowUp := len(chatHistory) > 0

	// If initial request and no topics were found, return an error
	if !isFollowUp && len(topicValues) == 0 {
		var errorMsg string
		hasDirect := len(topicData) > 0
		if hasPattern && hasDirect {
			errorMsg = fmt.Sprintf("No topics found matching pattern '%s' or provided in topicData", *topicPattern)
		} else if hasPattern {
			errorMsg = fmt.Sprintf("No topics found matching pattern '%s'", *topicPattern)
		} else if hasDirect {
			errorMsg = "No topics found in provided topicData"
		} else {
			errorMsg = "Either topicPattern or topicData must be provided"
		}
		return &generated.TopicAnalysisResponse{
			Response:       "",
			TopicsAnalyzed: 0,
			Model:          &modelName,
			Error:          &errorMsg,
		}, nil
	}

	// Format hierarchy
	tree := genai.FormatTopicsAsHierarchy(topicValues)

	// Format prompt
	defaultSysPrompt := "You are an Industrial IoT and MQTT expert analyzing live telemetry and topic structures.\nProvide clear, structured, and insightful answers based on the topic hierarchy and values."
	sysPrompt := defaultSysPrompt
	if systemPrompt != nil && strings.TrimSpace(*systemPrompt) != "" {
		sysPrompt = strings.TrimSpace(*systemPrompt)
	}

	var prompt string
	if !isFollowUp {
		prompt = fmt.Sprintf("%s\n\nMQTT TOPIC DATA HIERARCHY:\n%s\n\nUser Question:\n%s", sysPrompt, tree, question)
	} else {
		prompt = question
	}

	// Convert chat history
	var genAiHistory []genai.ChatMessage
	for _, ch := range chatHistory {
		if ch != nil {
			genAiHistory = append(genAiHistory, genai.ChatMessage{
				Role:    ch.Role,
				Content: ch.Content,
			})
		}
	}

	req := genai.Request{
		Prompt:      prompt,
		ChatHistory: genAiHistory,
	}

	resp, err := provider.Generate(ctx, req)
	if err != nil {
		errMsg := fmt.Sprintf("AI request failed: %s", err.Error())
		return &generated.TopicAnalysisResponse{
			Response:       "",
			TopicsAnalyzed: len(topicValues),
			Model:          &modelName,
			Error:          &errMsg,
		}, nil
	}

	var errPtr *string
	if resp.Error != "" {
		errPtr = &resp.Error
	}

	return &generated.TopicAnalysisResponse{
		Response:       resp.Response,
		TopicsAnalyzed: len(topicValues),
		Model:          &modelName,
		Error:          errPtr,
	}, nil
}

// GenAiQuery: proposeDataCatalog ---------------------------------------------

func (r *genAiQueryResolver) ProposeDataCatalog(
	ctx context.Context,
	_ *generated.GenAiQuery,
	archiveGroup *string,
	topicPattern string,
	prompt *string,
	maxTopics *int,
) (*generated.DataCatalogProposal, error) {
	if !r.Cfg.Features.GenAi && !r.Cfg.GenAI.Enabled {
		errMsg := "GenAI is not enabled or not configured"
		return &generated.DataCatalogProposal{
			Types:          []*generated.DataCatalogType{},
			Instances:      []*generated.DataCatalogInstance{},
			Relations:      []*generated.DataCatalogRelation{},
			TopicsAnalyzed: 0,
			Error:          &errMsg,
		}, nil
	}

	provider, err := r.getGenAiProvider(ctx)
	if err != nil || provider == nil {
		errMsg := "GenAI provider is not available"
		return &generated.DataCatalogProposal{
			Types:          []*generated.DataCatalogType{},
			Instances:      []*generated.DataCatalogInstance{},
			Relations:      []*generated.DataCatalogRelation{},
			TopicsAnalyzed: 0,
			Error:          &errMsg,
		}, nil
	}

	maxT := 100
	if maxTopics != nil && *maxTopics > 0 {
		maxT = *maxTopics
	}

	agName := "Default"
	if archiveGroup != nil && *archiveGroup != "" {
		agName = *archiveGroup
	}

	topicValues := make(map[string]string)
	store := r.lastValueStore(&agName)
	if store != nil {
		_ = store.FindMatchingMessages(ctx, topicPattern, func(m stores.BrokerMessage) bool {
			if len(topicValues) >= maxT {
				return false
			}
			val, ok := genai.FormatTopicValue(m.Payload, 500)
			if ok {
				topicValues[m.TopicName] = val
			}
			return true
		})
	}

	if len(topicValues) == 0 {
		errMsg := fmt.Sprintf("No topics found matching pattern '%s'", topicPattern)
		return &generated.DataCatalogProposal{
			Types:          []*generated.DataCatalogType{},
			Instances:      []*generated.DataCatalogInstance{},
			Relations:      []*generated.DataCatalogRelation{},
			TopicsAnalyzed: 0,
			Error:          &errMsg,
		}, nil
	}

	tree := genai.FormatTopicsAsHierarchy(topicValues)

	userPrompt := "Organize the topics into logical asset/device types and instances."
	if prompt != nil && strings.TrimSpace(*prompt) != "" {
		userPrompt = strings.TrimSpace(*prompt)
	}

	fullPrompt := fmt.Sprintf(`You are an Industrial IoT Semantic Data Architect.
Analyze the following MQTT topic hierarchy and sample payload values to construct a CESMII i3X Data Catalog proposal.

USER GUIDANCE: %s

MQTT TOPIC HIERARCHY & SAMPLE VALUES:
%s

INSTRUCTIONS:
1. Identify logical Object Types (e.g. Pump, Sensor, Machine, Line, Plant).
   - Each type must have: id (lowercase slug, e.g. "pump-type"), namespace (e.g. "default"), name, description, topicPattern (e.g. "plants/+/pumps/+"), and structure (a valid JSON Schema object with type="object", properties, etc.).
2. Identify concrete Object Instances:
   - Each instance must have: id (slug, e.g. "pump-01"), typeId (matching one of the types), name, baseTopic (the base MQTT topic for this instance), and properties (JSON object with metadata).
3. Identify Relations between instances and types (e.g. sourceId, targetId, relationType like "HasComponent", "HasParent", "ConnectedTo").
4. Provide a brief summary string.

Respond ONLY with valid JSON in this exact structure (no markdown formatting, no commentary):
{
  "summary": "Brief summary description",
  "types": [
    {
      "id": "string",
      "namespace": "string",
      "name": "string",
      "description": "string",
      "topicPattern": "string",
      "structure": { "type": "object", "properties": {} }
    }
  ],
  "instances": [
    {
      "id": "string",
      "typeId": "string",
      "name": "string",
      "baseTopic": "string",
      "properties": {}
    }
  ],
  "relations": [
    {
      "sourceId": "string",
      "targetId": "string",
      "relationType": "string"
    }
  ]
}`, userPrompt, tree)

	req := genai.Request{
		Prompt: fullPrompt,
	}

	resp, err := provider.Generate(ctx, req)
	if err != nil {
		errMsg := fmt.Sprintf("AI request failed: %s", err.Error())
		return &generated.DataCatalogProposal{
			Types:          []*generated.DataCatalogType{},
			Instances:      []*generated.DataCatalogInstance{},
			Relations:      []*generated.DataCatalogRelation{},
			TopicsAnalyzed: len(topicValues),
			Error:          &errMsg,
		}, nil
	}

	// Clean code fence if present
	cleanJSON := strings.TrimSpace(resp.Response)
	cleanJSON = strings.TrimPrefix(cleanJSON, "```json")
	cleanJSON = strings.TrimPrefix(cleanJSON, "```")
	cleanJSON = strings.TrimSuffix(cleanJSON, "```")
	cleanJSON = strings.TrimSpace(cleanJSON)

	type proposalJSON struct {
		Summary   string                        `json:"summary"`
		Types     []*generated.DataCatalogType     `json:"types"`
		Instances []*generated.DataCatalogInstance `json:"instances"`
		Relations []*generated.DataCatalogRelation `json:"relations"`
	}

	var parsed proposalJSON
	if err := json.Unmarshal([]byte(cleanJSON), &parsed); err != nil {
		errMsg := fmt.Sprintf("Failed to parse AI Data Catalog proposal: %s", err.Error())
		return &generated.DataCatalogProposal{
			Types:          []*generated.DataCatalogType{},
			Instances:      []*generated.DataCatalogInstance{},
			Relations:      []*generated.DataCatalogRelation{},
			TopicsAnalyzed: len(topicValues),
			Error:          &errMsg,
		}, nil
	}

	summaryPtr := &parsed.Summary
	return &generated.DataCatalogProposal{
		Types:          parsed.Types,
		Instances:      parsed.Instances,
		Relations:      parsed.Relations,
		TopicsAnalyzed: len(topicValues),
		Summary:        summaryPtr,
	}, nil
}

// Helpers --------------------------------------------------------------------

func (r *Resolver) getGenAiProvider(ctx context.Context) (genai.Provider, error) {
	if r.GenAi != nil {
		return r.GenAi, nil
	}
	p, err := genai.CreateAssistantProvider(ctx, r.Cfg.GenAI)
	if err != nil {
		return nil, err
	}
	r.GenAi = p
	return p, nil
}

func (r *Resolver) getConfigProviders() []*generated.GenAiProvider {
	out := []*generated.GenAiProvider{}
	p := r.Cfg.GenAI.Providers

	// Gemini
	if p.Gemini.ApiKey != "" {
		apiKeyMasked := "***"
		out = append(out, &generated.GenAiProvider{
			Name:        "Gemini",
			Type:        "gemini",
			APIKey:      &apiKeyMasked,
			Temperature: 0.7,
			Enabled:     true,
			Source:      "config",
			CreatedAt:   "1970-01-01T00:00:00Z",
			UpdatedAt:   "1970-01-01T00:00:00Z",
		})
	}

	// Claude
	if p.Claude.ApiKey != "" {
		apiKeyMasked := "***"
		out = append(out, &generated.GenAiProvider{
			Name:        "Claude",
			Type:        "claude",
			APIKey:      &apiKeyMasked,
			Temperature: 0.7,
			Enabled:     true,
			Source:      "config",
			CreatedAt:   "1970-01-01T00:00:00Z",
			UpdatedAt:   "1970-01-01T00:00:00Z",
		})
	}

	// OpenAI
	if p.OpenAI.ApiKey != "" || p.OpenAI.Endpoint != "" {
		var apiKeyMasked *string
		if p.OpenAI.ApiKey != "" {
			masked := "***"
			apiKeyMasked = &masked
		}
		var endpoint *string
		if p.OpenAI.Endpoint != "" {
			endpoint = &p.OpenAI.Endpoint
		}
		out = append(out, &generated.GenAiProvider{
			Name:        "OpenAI",
			Type:        "openai",
			APIKey:      apiKeyMasked,
			Endpoint:    endpoint,
			Temperature: 0.7,
			Enabled:     true,
			Source:      "config",
			CreatedAt:   "1970-01-01T00:00:00Z",
			UpdatedAt:   "1970-01-01T00:00:00Z",
		})
	}

	// Ollama
	if p.Ollama.BaseUrl != "" {
		out = append(out, &generated.GenAiProvider{
			Name:        "Ollama",
			Type:        "ollama",
			BaseURL:     &p.Ollama.BaseUrl,
			Temperature: 0.7,
			Enabled:     true,
			Source:      "config",
			CreatedAt:   "1970-01-01T00:00:00Z",
			UpdatedAt:   "1970-01-01T00:00:00Z",
		})
	}

	// LlamaCpp
	if p.LlamaCpp.Endpoint != "" {
		var apiKeyMasked *string
		if p.LlamaCpp.ApiKey != "" {
			masked := "***"
			apiKeyMasked = &masked
		}
		var model *string
		if p.LlamaCpp.Model != "" {
			model = &p.LlamaCpp.Model
		}
		out = append(out, &generated.GenAiProvider{
			Name:        "LlamaCpp",
			Type:        "llamacpp",
			Endpoint:    &p.LlamaCpp.Endpoint,
			APIKey:      apiKeyMasked,
			Model:       model,
			Temperature: 0.7,
			Enabled:     true,
			Source:      "config",
			CreatedAt:   "1970-01-01T00:00:00Z",
			UpdatedAt:   "1970-01-01T00:00:00Z",
		})
	}

	// OpenRouter
	if p.OpenRouter.ApiKey != "" || p.OpenRouter.Endpoint != "" {
		var apiKeyMasked *string
		if p.OpenRouter.ApiKey != "" {
			masked := "***"
			apiKeyMasked = &masked
		}
		var model *string
		if p.OpenRouter.Model != "" {
			model = &p.OpenRouter.Model
		}
		var endpoint *string
		if p.OpenRouter.Endpoint != "" {
			endpoint = &p.OpenRouter.Endpoint
		}
		effectiveType := "openrouter"
		if p.OpenRouter.Type == "decision" {
			effectiveType = "openrouter-decision"
		}
		out = append(out, &generated.GenAiProvider{
			Name:        "OpenRouter",
			Type:        effectiveType,
			APIKey:      apiKeyMasked,
			Model:       model,
			Endpoint:    endpoint,
			Temperature: 0.7,
			Enabled:     true,
			Source:      "config",
			CreatedAt:   "1970-01-01T00:00:00Z",
			UpdatedAt:   "1970-01-01T00:00:00Z",
		})
	}

	return out
}

func deviceToGenAiProvider(d stores.DeviceConfig) *generated.GenAiProvider {
	var input generated.GenAiProviderInput
	_ = json.Unmarshal([]byte(d.Config), &input)

	var apiKeyMasked *string
	if input.APIKey != nil && *input.APIKey != "" {
		masked := "***"
		apiKeyMasked = &masked
	}

	temp := 0.7
	if input.Temperature != nil {
		temp = *input.Temperature
	}

	return &generated.GenAiProvider{
		Name:           d.Name,
		Type:           input.Type,
		Model:          input.Model,
		APIKey:         apiKeyMasked,
		Endpoint:       input.Endpoint,
		ServiceVersion: input.ServiceVersion,
		BaseURL:        input.BaseURL,
		Temperature:    temp,
		MaxTokens:      input.MaxTokens,
		Enabled:        d.Enabled,
		Source:         "database",
		CreatedAt:      d.CreatedAt.Format(time.RFC3339),
		UpdatedAt:      d.UpdatedAt.Format(time.RFC3339),
	}
}

package genai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
)

// DeviceTypeProvider is the DeviceConfig type of providers created on the
// GenAI Providers page.
const DeviceTypeProvider = "genai_provider"

// providerRecord is the stored GenAiProviderInput of a provider.
type providerRecord struct {
	Type        string   `json:"type"`
	Model       *string  `json:"model"`
	APIKey      *string  `json:"apiKey"`
	Endpoint    *string  `json:"endpoint"`
	BaseURL     *string  `json:"baseUrl"`
	Temperature *float64 `json:"temperature"`
	MaxTokens   *int     `json:"maxTokens"`
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// ProviderByName creates the provider named on the GenAI Providers page:
// a stored provider, or one of the providers of the configuration file
// (Gemini, Claude, OpenAI, Ollama, LlamaCpp, OpenRouter).
func ProviderByName(ctx context.Context, cfg config.GenAIConfig, devices stores.DeviceConfigStore, name string) (Provider, error) {
	if devices != nil {
		d, err := devices.Get(ctx, name)
		if err != nil {
			return nil, err
		}
		if d != nil && d.Type == DeviceTypeProvider {
			if !d.Enabled {
				return nil, fmt.Errorf("GenAI provider %q is disabled", name)
			}
			var rec providerRecord
			if err := json.Unmarshal([]byte(d.Config), &rec); err != nil {
				return nil, fmt.Errorf("GenAI provider %q: %w", name, err)
			}
			p := ProviderParams{Provider: rec.Type, Model: str(rec.Model), ApiKey: resolveEnv(str(rec.APIKey)),
				Endpoint: resolveEnv(str(rec.Endpoint)), BaseUrl: resolveEnv(str(rec.BaseURL)), Temperature: 0.2}
			if rec.Temperature != nil {
				p.Temperature = *rec.Temperature
			}
			if rec.MaxTokens != nil {
				p.MaxTokens = *rec.MaxTokens
			}
			return CreateProvider(ctx, p)
		}
	}
	typ := strings.ToLower(name)
	switch typ {
	case "gemini", "claude", "openai", "ollama", "llamacpp", "openrouter":
	default:
		return nil, fmt.Errorf("unknown GenAI provider %q", name)
	}
	c := cfg
	c.Enabled = true
	if !strings.EqualFold(c.Assistant.Provider, typ) {
		c.Assistant.Model = ""
		c.Assistant.Endpoint = ""
	}
	c.Assistant.Provider = typ
	return CreateAssistantProvider(ctx, c)
}

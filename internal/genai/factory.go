package genai

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/anthropic"
	"github.com/tmc/langchaingo/llms/googleai"
	"github.com/tmc/langchaingo/llms/ollama"
	"github.com/tmc/langchaingo/llms/openai"

	"monstermq.io/edge/internal/config"
)

// ProviderParams specifies parameters for constructing an LLM provider.
type ProviderParams struct {
	Provider       string
	Model          string
	ApiKey         string
	Endpoint       string
	BaseUrl        string
	Temperature    float64
	MaxTokens      int
	DocsPath       string
	TimeoutSeconds int
}

// CreateAssistantProvider creates the primary assistant Provider from broker configuration.
func CreateAssistantProvider(ctx context.Context, cfg config.GenAIConfig) (Provider, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	providerType := strings.ToLower(strings.TrimSpace(cfg.Assistant.Provider))
	if providerType == "" {
		providerType = "gemini"
	}

	modelName := strings.TrimSpace(cfg.Assistant.Model)
	temperature := cfg.Assistant.Temperature
	if temperature <= 0 {
		temperature = 0.7
	}
	docsPath := cfg.Assistant.DocsPath
	if docsPath == "" {
		docsPath = "docs"
	}

	params := ProviderParams{
		Provider:    providerType,
		Model:       modelName,
		Temperature: temperature,
		DocsPath:    docsPath,
	}

	switch providerType {
	case "gemini":
		params.ApiKey = resolveEnv(cfg.Providers.Gemini.ApiKey)
		if params.ApiKey == "" {
			params.ApiKey = os.Getenv("GEMINI_API_KEY")
		}
		if params.Model == "" {
			params.Model = "gemini-2.5-flash"
		}
	case "claude":
		params.ApiKey = resolveEnv(cfg.Providers.Claude.ApiKey)
		if params.ApiKey == "" {
			params.ApiKey = os.Getenv("ANTHROPIC_API_KEY")
		}
		if params.Model == "" {
			params.Model = "claude-3-7-sonnet-20250219"
		}
	case "openai":
		params.ApiKey = resolveEnv(cfg.Providers.OpenAI.ApiKey)
		if params.ApiKey == "" {
			params.ApiKey = os.Getenv("OPENAI_API_KEY")
		}
		params.Endpoint = resolveEnv(cfg.Providers.OpenAI.Endpoint)
		params.TimeoutSeconds = cfg.Providers.OpenAI.TimeoutSeconds
		if params.Model == "" {
			params.Model = "gpt-4o"
		}
	case "ollama":
		params.BaseUrl = resolveEnv(cfg.Providers.Ollama.BaseUrl)
		if params.BaseUrl == "" {
			params.BaseUrl = "http://localhost:11434"
		}
		params.TimeoutSeconds = cfg.Providers.Ollama.TimeoutSeconds
		if params.Model == "" {
			params.Model = "llama3"
		}
	case "llamacpp":
		params.Endpoint = resolveEnv(cfg.Providers.LlamaCpp.Endpoint)
		if params.Endpoint == "" {
			params.Endpoint = "http://localhost:8080/v1"
		}
		params.ApiKey = resolveEnv(cfg.Providers.LlamaCpp.ApiKey)
		params.TimeoutSeconds = cfg.Providers.LlamaCpp.TimeoutSeconds
		if params.Model == "" && cfg.Providers.LlamaCpp.Model != "" {
			params.Model = cfg.Providers.LlamaCpp.Model
		}
	case "openrouter":
		params.ApiKey = resolveEnv(cfg.Providers.OpenRouter.ApiKey)
		if params.ApiKey == "" {
			params.ApiKey = os.Getenv("OPENROUTER_API_KEY")
		}
		params.Endpoint = resolveEnv(cfg.Providers.OpenRouter.Endpoint)
		if params.Endpoint == "" {
			params.Endpoint = "https://openrouter.ai/api/v1"
		}
		params.TimeoutSeconds = cfg.Providers.OpenRouter.TimeoutSeconds
		if params.Model == "" && cfg.Providers.OpenRouter.Model != "" {
			params.Model = cfg.Providers.OpenRouter.Model
		}
	}

	// Custom endpoint override from assistant
	if cfg.Assistant.Endpoint != "" {
		params.Endpoint = resolveEnv(cfg.Assistant.Endpoint)
	}

	return CreateProvider(ctx, params)
}

// CreateProvider creates a Provider implementation from ProviderParams.
func CreateProvider(ctx context.Context, params ProviderParams) (Provider, error) {
	providerType := strings.ToLower(strings.TrimSpace(params.Provider))

	var model llms.Model
	var err error

	switch providerType {
	case "gemini":
		var opts []googleai.Option
		if params.ApiKey != "" {
			opts = append(opts, googleai.WithAPIKey(params.ApiKey))
		}
		if params.Model != "" {
			opts = append(opts, googleai.WithDefaultModel(params.Model))
		}
		model, err = googleai.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create Google Gemini client: %w", err)
		}

	case "claude":
		var opts []anthropic.Option
		if params.ApiKey != "" {
			opts = append(opts, anthropic.WithToken(params.ApiKey))
		}
		if params.Model != "" {
			opts = append(opts, anthropic.WithModel(params.Model))
		}
		if params.Endpoint != "" {
			opts = append(opts, anthropic.WithBaseURL(params.Endpoint))
		}
		model, err = anthropic.New(opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create Anthropic Claude client: %w", err)
		}

	case "openai":
		var opts []openai.Option
		if params.ApiKey != "" {
			opts = append(opts, openai.WithToken(params.ApiKey))
		}
		if params.Model != "" {
			opts = append(opts, openai.WithModel(params.Model))
		}
		if params.Endpoint != "" {
			opts = append(opts, openai.WithBaseURL(params.Endpoint))
		}
		model, err = openai.New(opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OpenAI client: %w", err)
		}

	case "ollama":
		var opts []ollama.Option
		serverURL := params.BaseUrl
		if serverURL == "" {
			serverURL = params.Endpoint
		}
		if serverURL == "" {
			serverURL = "http://localhost:11434"
		}
		opts = append(opts, ollama.WithServerURL(serverURL))
		if params.Model != "" {
			opts = append(opts, ollama.WithModel(params.Model))
		}
		model, err = ollama.New(opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create Ollama client: %w", err)
		}

	case "llamacpp", "openrouter":
		// Both are OpenAI-compatible endpoints
		var opts []openai.Option
		if params.ApiKey != "" {
			opts = append(opts, openai.WithToken(params.ApiKey))
		}
		if params.Model != "" {
			opts = append(opts, openai.WithModel(params.Model))
		}
		baseURL := params.Endpoint
		if baseURL == "" {
			baseURL = params.BaseUrl
		}
		if baseURL != "" {
			opts = append(opts, openai.WithBaseURL(baseURL))
		}
		model, err = openai.New(opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create %s client: %w", params.Provider, err)
		}

	default:
		return nil, fmt.Errorf("unsupported AI provider: %s", params.Provider)
	}

	adapter := NewLangChainAdapter(
		model,
		params.Provider,
		params.Model,
		params.DocsPath,
		params.Temperature,
		params.MaxTokens,
	)

	return adapter, nil
}

func resolveEnv(val string) string {
	val = strings.TrimSpace(val)
	if strings.HasPrefix(val, "${") && strings.HasSuffix(val, "}") {
		envKey := strings.TrimSuffix(strings.TrimPrefix(val, "${"), "}")
		return os.Getenv(envKey)
	}
	return val
}

package genai

import (
	"context"
	"testing"

	"monstermq.io/edge/internal/config"
)

func TestFactoryCreateProviders(t *testing.T) {
	ctx := context.Background()

	// 1. OpenAI compatible (OpenAI, OpenRouter, llama.cpp)
	t.Run("openai-compatible", func(t *testing.T) {
		p, err := CreateProvider(ctx, ProviderParams{
			Provider:    "openai",
			Model:       "gpt-4o",
			ApiKey:      "sk-test",
			Endpoint:    "http://localhost:8080/v1",
			Temperature: 0.7,
		})
		if err != nil {
			t.Fatalf("failed to create openai provider: %v", err)
		}
		if p.ProviderName() != "openai" || p.ModelName() != "gpt-4o" {
			t.Fatalf("unexpected provider details: %s / %s", p.ProviderName(), p.ModelName())
		}
	})

	t.Run("openrouter", func(t *testing.T) {
		p, err := CreateProvider(ctx, ProviderParams{
			Provider:    "openrouter",
			Model:       "anthropic/claude-3.5-sonnet",
			ApiKey:      "sk-test",
			Endpoint:    "https://openrouter.ai/api/v1",
			Temperature: 0.5,
		})
		if err != nil {
			t.Fatalf("failed to create openrouter provider: %v", err)
		}
		if p.ProviderName() != "openrouter" {
			t.Fatalf("expected openrouter provider name")
		}
	})

	t.Run("llamacpp", func(t *testing.T) {
		p, err := CreateProvider(ctx, ProviderParams{
			Provider: "llamacpp",
			Model:    "llama-3-8b",
			Endpoint: "http://localhost:8080/v1",
		})
		if err != nil {
			t.Fatalf("failed to create llamacpp provider: %v", err)
		}
		if p.ProviderName() != "llamacpp" {
			t.Fatalf("expected llamacpp provider name")
		}
	})

	t.Run("ollama", func(t *testing.T) {
		p, err := CreateProvider(ctx, ProviderParams{
			Provider: "ollama",
			Model:    "llama3",
			BaseUrl:  "http://localhost:11434",
		})
		if err != nil {
			t.Fatalf("failed to create ollama provider: %v", err)
		}
		if p.ProviderName() != "ollama" {
			t.Fatalf("expected ollama provider name")
		}
	})

	t.Run("claude", func(t *testing.T) {
		p, err := CreateProvider(ctx, ProviderParams{
			Provider: "claude",
			Model:    "claude-3-7-sonnet-20250219",
			ApiKey:   "sk-ant-test",
		})
		if err != nil {
			t.Fatalf("failed to create claude provider: %v", err)
		}
		if p.ProviderName() != "claude" {
			t.Fatalf("expected claude provider name")
		}
	})

	t.Run("assistant-disabled", func(t *testing.T) {
		cfg := config.GenAIConfig{
			Enabled: false,
		}
		p, err := CreateAssistantProvider(ctx, cfg)
		if err != nil {
			t.Fatalf("unexpected error when disabled: %v", err)
		}
		if p != nil {
			t.Fatalf("expected nil provider when disabled")
		}
	})

	t.Run("unsupported", func(t *testing.T) {
		_, err := CreateProvider(ctx, ProviderParams{
			Provider: "non-existent-ai",
		})
		if err == nil {
			t.Fatalf("expected error for unsupported provider")
		}
	})
}

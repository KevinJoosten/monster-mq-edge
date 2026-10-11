package genai

import (
	"context"
)

// Provider represents a generative AI provider backend.
type Provider interface {
	ProviderName() string
	ModelName() string
	Generate(ctx context.Context, req Request) (*Response, error)
	IsReady() bool
	Close() error
}

// Request encapsulates a generation request to the AI model.
type Request struct {
	Prompt      string
	Context     string
	Docs        []string
	ChatHistory []ChatMessage
}

// ChatMessage represents a single message in a multi-turn conversation.
type ChatMessage struct {
	Role    string // "user", "assistant", "system"
	Content string
}

// Response contains the generated output from the provider.
type Response struct {
	Response string
	Model    string
	Error    string
}

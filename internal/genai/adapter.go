package genai

import (
	"context"
	"fmt"
	"strings"

	"github.com/tmc/langchaingo/llms"
)

// LangChainAdapter adapts a LangChainGo llms.Model to the genai.Provider interface.
type LangChainAdapter struct {
	model llms.Model
	// toolModel serves tool calling when the main client cannot (Ollama:
	// its OpenAI-compatible /v1 endpoint through the openai client).
	toolModel    llms.Model
	providerName string
	modelName    string
	docsPath     string
	temperature  float64
	maxTokens    int
}

// NewLangChainAdapter creates a new provider adapter wrapping an llms.Model.
func NewLangChainAdapter(model llms.Model, providerName, modelName, docsPath string, temperature float64, maxTokens int) *LangChainAdapter {
	return &LangChainAdapter{
		model:        model,
		providerName: providerName,
		modelName:    modelName,
		docsPath:     docsPath,
		temperature:  temperature,
		maxTokens:    maxTokens,
	}
}

func (a *LangChainAdapter) ProviderName() string {
	return a.providerName
}

func (a *LangChainAdapter) ModelName() string {
	return a.modelName
}

func (a *LangChainAdapter) IsReady() bool {
	return a.model != nil
}

func (a *LangChainAdapter) Close() error {
	return nil
}

func (a *LangChainAdapter) Generate(ctx context.Context, req Request) (*Response, error) {
	if a.model == nil {
		return &Response{
			Response: "",
			Model:    a.modelName,
			Error:    "Provider model is not initialized",
		}, nil
	}

	var messages []llms.MessageContent

	// Add chat history if present
	for _, msg := range req.ChatHistory {
		var role llms.ChatMessageType
		switch strings.ToLower(msg.Role) {
		case "assistant", "ai":
			role = llms.ChatMessageTypeAI
		case "system":
			role = llms.ChatMessageTypeSystem
		default:
			role = llms.ChatMessageTypeHuman
		}
		messages = append(messages, llms.TextParts(role, msg.Content))
	}

	// Build the main prompt with optional context and documentation
	var promptBuilder strings.Builder

	if req.Context != "" {
		promptBuilder.WriteString("Context:\n")
		promptBuilder.WriteString(req.Context)
		promptBuilder.WriteString("\n\n")
	}

	if len(req.Docs) > 0 {
		docContext, err := LoadDocsContext(a.docsPath, req.Docs)
		if err != nil {
			return &Response{
				Response: "",
				Model:    a.modelName,
				Error:    fmt.Sprintf("Failed to load documentation: %s", err.Error()),
			}, nil
		}
		if docContext != "" {
			promptBuilder.WriteString("Documentation:\n")
			promptBuilder.WriteString(docContext)
			promptBuilder.WriteString("\n\n")
		}
	}

	promptBuilder.WriteString(req.Prompt)

	messages = append(messages, llms.TextParts(llms.ChatMessageTypeHuman, promptBuilder.String()))

	var opts []llms.CallOption
	if a.temperature > 0 {
		opts = append(opts, llms.WithTemperature(a.temperature))
	}
	if a.maxTokens > 0 {
		opts = append(opts, llms.WithMaxTokens(a.maxTokens))
	}

	resp, err := a.model.GenerateContent(ctx, messages, opts...)
	if err != nil {
		return &Response{
			Response: "",
			Model:    a.modelName,
			Error:    fmt.Sprintf("AI request failed: %s", err.Error()),
		}, nil
	}

	if len(resp.Choices) == 0 {
		return &Response{
			Response: "",
			Model:    a.modelName,
			Error:    "No response generated from AI provider",
		}, nil
	}

	return &Response{
		Response: resp.Choices[0].Content,
		Model:    a.modelName,
	}, nil
}

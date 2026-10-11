package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"monstermq.io/edge/internal/config"
)

// setupMockLLMServer creates an httptest server simulating OpenAI /chat/completions endpoint.
func setupMockLLMServer(t *testing.T) (*httptest.Server, *[]map[string]any) {
	t.Helper()
	var recordedRequests []map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}

		var reqBody map[string]any
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		recordedRequests = append(recordedRequests, reqBody)

		messages, _ := reqBody["messages"].([]any)
		lastContent := ""
		for _, m := range messages {
			if mmap, ok := m.(map[string]any); ok {
				if c, ok := mmap["content"].(string); ok {
					lastContent = c
				}
			}
		}

		responseContent := "Default mock AI response"
		if strings.Contains(lastContent, "CESMII i3X Data Catalog proposal") {
			responseContent = `{
  "summary": "Proposal for industrial telemetry",
  "types": [
    {
      "id": "pump-type",
      "namespace": "default",
      "name": "Pump",
      "description": "Industrial pump",
      "topicPattern": "plants/+/pumps/+",
      "structure": { "type": "object", "properties": { "rpm": { "type": "number" } } }
    }
  ],
  "instances": [
    {
      "id": "pump-01",
      "typeId": "pump-type",
      "name": "Feed Pump 1",
      "baseTopic": "plants/site1/pumps/p1",
      "properties": {}
    }
  ],
  "relations": [
    {
      "sourceId": "pump-01",
      "targetId": "pump-type",
      "relationType": "InstanceOf"
    }
  ]
}`
		} else if strings.Contains(lastContent, "MQTT TOPIC DATA HIERARCHY") || strings.Contains(lastContent, "User Question") {
			responseContent = "Telemetry analysis: observed 2 topics with stable readings."
		} else if strings.Contains(lastContent, "Write a filter script") {
			responseContent = "def on_message(topic, payload): return payload"
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      "chatcmpl-test",
			"object":  "chat.completion",
			"created": time.Now().Unix(),
			"model":   "mock-gpt-4o",
			"choices": []map[string]any{
				{
					"index": 0,
					"message": map[string]any{
						"role":    "assistant",
						"content": responseContent,
					},
					"finish_reason": "stop",
				},
			},
		})
	}))

	return server, &recordedRequests
}

// AC-01: Gating when GenAI is disabled.
func TestGenAiSubsystemDisabled(t *testing.T) {
	mqttPort := 21950
	gqlPort := 21951

	srv, gqlURL := startWithGraphQL(t, mqttPort, gqlPort, func(c *config.Config) {
		c.Features.GenAi = false
		c.GenAI.Enabled = false
		c.SQLite.Path = filepath.Join(t.TempDir(), "genai_disabled.db")
	})
	defer srv.Close()

	// 1. enabledFeatures omits GenAi
	resp := gqlQuery(t, gqlURL, `query { broker { enabledFeatures } }`, nil)
	features := resp["broker"].(map[string]any)["enabledFeatures"].([]any)
	for _, f := range features {
		if f.(string) == "GenAi" {
			t.Fatalf("expected GenAi to be absent from enabledFeatures when disabled")
		}
	}

	// 2. Query genai returns null
	resp = gqlQuery(t, gqlURL, `query { genai { generate(prompt: "hello") { response } } }`, nil)
	if resp["genai"] != nil {
		t.Fatalf("expected genai query to be null when disabled, got %v", resp["genai"])
	}

	// 3. Query genAiProviders returns empty
	resp = gqlQuery(t, gqlURL, `query { genAiProviders { name } }`, nil)
	providers := resp["genAiProviders"].([]any)
	if len(providers) != 0 {
		t.Fatalf("expected 0 genAiProviders when disabled, got %d", len(providers))
	}
}

// AC-02, AC-04: Config parsing and dynamic provider CRUD.
func TestGenAiProviderCRUD(t *testing.T) {
	mqttPort := 21952
	gqlPort := 21953

	srv, gqlURL := startWithGraphQL(t, mqttPort, gqlPort, func(c *config.Config) {
		c.Features.GenAi = true
		c.GenAI.Enabled = true
		c.GenAI.Providers.OpenAI.ApiKey = "sk-config-key"
		c.GenAI.Providers.OpenAI.Endpoint = "http://localhost:8080/v1"
		c.GenAI.Providers.Ollama.BaseUrl = "http://localhost:11434"
		c.SQLite.Path = filepath.Join(t.TempDir(), "genai_crud.db")
	})
	defer srv.Close()

	// 1. Verify enabledFeatures includes GenAi
	resp := gqlQuery(t, gqlURL, `query { broker { enabledFeatures } }`, nil)
	features := resp["broker"].(map[string]any)["enabledFeatures"].([]any)
	hasGenAi := false
	for _, f := range features {
		if f.(string) == "GenAi" {
			hasGenAi = true
			break
		}
	}
	if !hasGenAi {
		t.Fatalf("expected GenAi in enabledFeatures, got %v", features)
	}

	// 2. Initial providers should contain config providers (OpenAI, Ollama)
	resp = gqlQuery(t, gqlURL, `query { genAiProviders { name type source apiKey endpoint baseUrl } }`, nil)
	providers := resp["genAiProviders"].([]any)
	if len(providers) < 2 {
		t.Fatalf("expected at least 2 config providers, got %d", len(providers))
	}

	foundOpenAI := false
	for _, p := range providers {
		pmap := p.(map[string]any)
		if pmap["name"] == "OpenAI" {
			foundOpenAI = true
			if pmap["source"] != "config" {
				t.Errorf("expected OpenAI provider source to be 'config', got %v", pmap["source"])
			}
			if pmap["apiKey"] != "***" {
				t.Errorf("expected masked apiKey '***', got %v", pmap["apiKey"])
			}
		}
	}
	if !foundOpenAI {
		t.Fatalf("OpenAI provider not found in config providers")
	}

	// 3. Create a dynamic database provider
	createMutation := `
mutation CreateProvider($name: String!, $input: GenAiProviderInput!) {
    genAiProvider {
        create(name: $name, input: $input) {
            name
            type
            model
            apiKey
            endpoint
            temperature
            enabled
            source
        }
    }
}`
	createVars := map[string]any{
		"name": "custom_cloud",
		"input": map[string]any{
			"type":        "openai",
			"model":       "gpt-4o-mini",
			"apiKey":      "sk-secret-12345",
			"endpoint":    "https://api.openai.com/v1",
			"temperature": 0.5,
			"enabled":     true,
		},
	}
	resp = gqlQuery(t, gqlURL, createMutation, createVars)
	created := resp["genAiProvider"].(map[string]any)["create"].(map[string]any)
	if created["name"] != "custom_cloud" || created["source"] != "database" {
		t.Fatalf("unexpected created provider: %v", created)
	}
	if created["apiKey"] != "***" {
		t.Fatalf("expected masked apiKey, got %v", created["apiKey"])
	}

	// 4. Query single provider by name
	singleResp := gqlQuery(t, gqlURL, `query GetP($name: String!) { genAiProvider(name: $name) { name model source } }`, map[string]any{"name": "custom_cloud"})
	singleP := singleResp["genAiProvider"].(map[string]any)
	if singleP["name"] != "custom_cloud" || singleP["model"] != "gpt-4o-mini" {
		t.Fatalf("unexpected provider returned: %v", singleP)
	}

	// 5. Update provider
	updateMutation := `
mutation UpdateProvider($name: String!, $input: GenAiProviderInput!) {
    genAiProvider {
        update(name: $name, input: $input) {
            name
            model
            apiKey
            temperature
        }
    }
}`
	updateVars := map[string]any{
		"name": "custom_cloud",
		"input": map[string]any{
			"type":        "openai",
			"model":       "gpt-4o-updated",
			"apiKey":      "***", // Should preserve existing secret
			"temperature": 0.2,
		},
	}
	resp = gqlQuery(t, gqlURL, updateMutation, updateVars)
	updated := resp["genAiProvider"].(map[string]any)["update"].(map[string]any)
	if updated["model"] != "gpt-4o-updated" {
		t.Fatalf("expected updated model 'gpt-4o-updated', got %v", updated["model"])
	}
	if updated["apiKey"] != "***" {
		t.Fatalf("expected preserved masked apiKey '***', got %v", updated["apiKey"])
	}

	// 6. Delete provider
	deleteMutation := `
mutation DeleteProvider($name: String!) {
    genAiProvider {
        delete(name: $name)
    }
}`
	resp = gqlQuery(t, gqlURL, deleteMutation, map[string]any{"name": "custom_cloud"})
	deleted := resp["genAiProvider"].(map[string]any)["delete"].(bool)
	if !deleted {
		t.Fatalf("expected delete to return true")
	}

	// 7. Verify provider is gone
	resp = gqlQuery(t, gqlURL, `query GetP($name: String!) { genAiProvider(name: $name) { name } }`, map[string]any{"name": "custom_cloud"})
	if resp["genAiProvider"] != nil {
		t.Fatalf("expected deleted provider to be nil, got %v", resp["genAiProvider"])
	}
}

// AC-05, AC-06, AC-07, AC-09: AI generation, topic analysis, and data catalog proposal with mock LLM.
func TestGenAiOperationsWithMock(t *testing.T) {
	mockServer, requests := setupMockLLMServer(t)
	defer mockServer.Close()

	mqttPort := 21954
	gqlPort := 21955

	srv, gqlURL := startWithGraphQL(t, mqttPort, gqlPort, func(c *config.Config) {
		c.Features.GenAi = true
		c.GenAI.Enabled = true
		c.GenAI.Assistant.Provider = "openai"
		c.GenAI.Assistant.Model = "mock-gpt-4o"
		c.GenAI.Assistant.DocsPath = t.TempDir()
		c.GenAI.Providers.OpenAI.ApiKey = "test-token"
		c.GenAI.Providers.OpenAI.Endpoint = mockServer.URL // Direct mock endpoint
		c.SQLite.Path = filepath.Join(t.TempDir(), "genai_ops.db")
	})
	defer srv.Close()

	// 1. Test generate
	generateQuery := `
query GenerateAI($prompt: String!, $context: String) {
    genai {
        generate(prompt: $prompt, context: $context) {
            response
            model
            error
        }
    }
}`
	resp := gqlQuery(t, gqlURL, generateQuery, map[string]any{
		"prompt":  "Write a filter script",
		"context": "Filter for temperature > 50",
	})
	genResp := resp["genai"].(map[string]any)["generate"].(map[string]any)
	if !strings.Contains(genResp["response"].(string), "def on_message") {
		t.Fatalf("unexpected generate response: %v", genResp)
	}

	// 2. Publish MQTT topic to populate LastValueStore
	opts := mqtt.NewClientOptions().AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", mqttPort)).SetClientID("test-pub")
	mc := mqtt.NewClient(opts)
	tok := mc.Connect()
	if !tok.WaitTimeout(3 * time.Second) || tok.Error() != nil {
		t.Fatalf("mqtt connect failed: %v", tok.Error())
	}
	defer mc.Disconnect(100)

	mc.Publish("plants/site1/pumps/p1/status", 0, false, `{"rpm": 1450, "temp": 42.1}`).Wait()
	mc.Publish("plants/site1/pumps/p1/power", 0, false, `{"voltage": 230, "current": 12.5}`).Wait()
	time.Sleep(100 * time.Millisecond) // Give time for LastValueStore update

	// 3. Test analyzeTopics with dual source (LastValueStore pattern + direct topicData)
	analyzeQuery := `
query Analyze($pattern: String, $data: [TopicDataInput!], $question: String!, $history: [ChatMessage!]) {
    genai {
        analyzeTopics(topicPattern: $pattern, topicData: $data, question: $question, chatHistory: $history) {
            response
            topicsAnalyzed
            model
            error
        }
    }
}`
	analyzeVars := map[string]any{
		"pattern": "plants/#",
		"data": []map[string]any{
			{"topic": "virtual/sensor/humidity", "value": "65%"},
		},
		"question": "What is the status of the plant?",
		"history":  []any{},
	}
	resp = gqlQuery(t, gqlURL, analyzeQuery, analyzeVars)
	analyzeResp := resp["genai"].(map[string]any)["analyzeTopics"].(map[string]any)
	if analyzeResp["topicsAnalyzed"].(float64) < 2 {
		t.Fatalf("expected at least 2 topics analyzed, got %v", analyzeResp["topicsAnalyzed"])
	}
	if !strings.Contains(analyzeResp["response"].(string), "Telemetry analysis") {
		t.Fatalf("unexpected analyzeTopics response: %v", analyzeResp)
	}

	// Verify the mock server received the ASCII tree in the prompt
	lastReq := (*requests)[len(*requests)-1]
	messages := lastReq["messages"].([]any)
	foundTree := false
	for _, m := range messages {
		mmap := m.(map[string]any)
		if c, ok := mmap["content"].(string); ok {
			if strings.Contains(c, "MQTT TOPIC DATA HIERARCHY") && strings.Contains(c, "humidity = 65%") {
				foundTree = true
				break
			}
		}
	}
	if !foundTree {
		t.Fatalf("expected mock request to contain ASCII topic hierarchy")
	}

	// 4. Test follow-up analyzeTopics (chatHistory present -> topic tree omitted)
	followUpVars := map[string]any{
		"question": "Are there any alarms?",
		"history": []map[string]any{
			{"role": "user", "content": "What is the status of the plant?"},
			{"role": "assistant", "content": "Telemetry analysis: observed 2 topics."},
		},
	}
	resp = gqlQuery(t, gqlURL, analyzeQuery, followUpVars)
	followUpResp := resp["genai"].(map[string]any)["analyzeTopics"].(map[string]any)
	if followUpResp["response"] == "" {
		t.Fatalf("expected non-empty follow-up response")
	}

	// 5. Test proposeDataCatalog
	catalogQuery := `
query ProposeCatalog($pattern: String!) {
    genai {
        proposeDataCatalog(topicPattern: $pattern) {
            types {
                id
                name
                structure
            }
            instances {
                id
                typeId
                baseTopic
            }
            relations {
                sourceId
                targetId
                relationType
            }
            summary
            error
        }
    }
}`
	resp = gqlQuery(t, gqlURL, catalogQuery, map[string]any{"pattern": "plants/#"})
	catResp := resp["genai"].(map[string]any)["proposeDataCatalog"].(map[string]any)
	if catResp["summary"] == nil || !strings.Contains(catResp["summary"].(string), "Proposal") {
		t.Fatalf("unexpected catalog proposal summary: %v", catResp)
	}
	typesList := catResp["types"].([]any)
	if len(typesList) != 1 || typesList[0].(map[string]any)["id"] != "pump-type" {
		t.Fatalf("expected pump-type in proposal types, got %v", typesList)
	}
	instList := catResp["instances"].([]any)
	if len(instList) != 1 || instList[0].(map[string]any)["id"] != "pump-01" {
		t.Fatalf("expected pump-01 in proposal instances, got %v", instList)
	}

	// 6. Test empty prompt returns error object without GraphQL query failure
	resp = gqlQuery(t, gqlURL, generateQuery, map[string]any{"prompt": ""})
	emptyPromptResp := resp["genai"].(map[string]any)["generate"].(map[string]any)
	if emptyPromptResp["error"] == nil || !strings.Contains(emptyPromptResp["error"].(string), "Prompt is required") {
		t.Fatalf("expected prompt required error, got %v", emptyPromptResp)
	}

	// 7. Test analyzeTopics with neither pattern nor data
	noTopicsResp := gqlQuery(t, gqlURL, analyzeQuery, map[string]any{
		"question": "what is this?",
	})
	noTopicsErr := noTopicsResp["genai"].(map[string]any)["analyzeTopics"].(map[string]any)
	if noTopicsErr["error"] == nil || !strings.Contains(noTopicsErr["error"].(string), "Either topicPattern or topicData must be provided") {
		t.Fatalf("expected either pattern or data error, got %v", noTopicsErr)
	}
}

# Plan: GenAI & Provider Management on MonsterMQ Edge

Status: proposed, awaiting owner sign-off (2026-10-11).

Edge gets the main broker's GenAI subsystem as a 1:1 port: same configuration
shape, same wire/GraphQL schema, and same runtime capabilities (assistant
code generation, topic hierarchy analysis, and data catalog proposal).
The implementation is native Go, evaluated against official SDKs and multi-provider
frameworks.

Companion references:
- Main broker GraphQL schemas: `../main/broker/src/main/resources/schema-genai.graphqls`
  and `../main/broker/src/main/resources/schema-genai-providers.graphqls`.
- Main broker config schema: `../main/broker/yaml-json-schema.json` (`GenAI` block, `Features.GenAi`).
- Main broker resolvers: `../main/broker/src/main/kotlin/graphql/GenAiResolver.kt`,
  `GenAiProviderQueries.kt`, and `GenAiProviderMutations.kt`.
- Main broker provider engine: `../main/broker/src/main/kotlin/genai/` and `../main/broker/src/main/kotlin/agents/LangChain4jFactory.kt`.

---

## 1. Context & Motivation

### 1.1 Why GenAI is needed on Edge

MonsterMQ Edge is designed as a lightweight, single-binary broker for edge deployments
(e.g., Raspberry Pi, Siemens IPCs, industrial gateways). The web dashboard
(`monster-mq-dashboard`) connects seamlessly to either a central MonsterMQ broker or
an edge broker.

Currently, the dashboard queries `window.graphqlClient.hasQueryField('genai')`. Because
Edge omits GenAI, the dashboard hides AI-assisted capabilities when connected to Edge:
- AI-assisted script generation and troubleshooting in the Script Editor.
- Natural language MQTT topic analysis in the Topic Browser.
- Semantic Data Catalog proposal generation (CESMII i3X) from live topic trees.
- GenAI Provider configuration management (`genAiProviders`, `genAiProvider`).

Industrial edge gateways frequently need local or cloud LLM connectivity:
- Local edge models via **Ollama** or **llama.cpp** running on the same gateway or LAN
  (zero cloud dependency, data privacy).
- Cloud models via **Google Gemini**, **Anthropic Claude**, **OpenAI**, or **OpenRouter**
  for high-capability analysis.

### 1.2 Non-Negotiable Parity Rules

Per `AGENTS.md`:
1. **GraphQL Schema Parity**: All types, fields, inputs, arguments, and nullabilities
   must be a strict 1:1 match with the Kotlin broker's schema.
2. **Configuration Parity**: The `config.yaml` `GenAI` block and `Features.GenAi` flag
   must mirror the JVM broker's configuration schema exactly, including environment
   variable substitution (`${ENV_VAR}`).
3. **Storage Parity**: Dynamic GenAI providers created via the dashboard/GraphQL use the
   existing `DeviceConfigStore` (`DeviceConfig` with type `"genai_provider"`), matching
   the SQLite, PostgreSQL, and MongoDB database shapes.

---

## 2. Go Framework Evaluation

In the Java broker, MonsterMQ uses **LangChain4j** (`dev.langchain4j`) to unify
access to Gemini, Claude, OpenAI, Ollama, and OpenAI-compatible gateways.

For the Go port on Edge, we evaluate the leading framework candidates:

| Criterion | Option A: LangChainGo (`github.com/tmc/langchaingo`) | Option B: Official Vendor SDKs with Internal `genai.Provider` Adapter | Option C: Bare HTTP REST Clients |
| :--- | :--- | :--- | :--- |
| **Description** | Go port of the LangChain ecosystem; unified `llms.Model` interface. | Official SDKs from Google, OpenAI, and Anthropic wrapped by an internal interface. | Hand-rolled HTTP clients per provider using `net/http`. |
| **Provider Support** | Direct subpackages: `llms/googleai`, `llms/openai`, `llms/anthropic`, `llms/ollama`. | `google.golang.org/genai`, `github.com/openai/openai-go`, `github.com/anthropics/anthropic-sdk-go`. | Manual JSON payloads for each API. |
| **OpenAI-Compatible** | Handled natively by `openai.WithBaseURL(...)` (supports OpenRouter, llama.cpp, LocalAI). | Handled natively by `openai-go` with `option.WithBaseURL(...)`. | Manual JSON endpoints. |
| **Architecture Fit** | 1:1 conceptual match with JVM's LangChain4j. | Lean, official, minimal transitive dependencies; single-binary friendly. | Zero dependencies, but high maintenance on API changes. |
| **Stability & Maintenance** | Community-driven; popular but occasional schema drifts. | Maintained directly by Google, OpenAI, Anthropic; production SLAs. | Self-maintained. |
| **Binary Footprint** | Moderate (imports LangChainGo toolchain). | Low to moderate (clean vendor packages). | Lowest. |

### Recommended Decision: **Option B (Official Vendor SDKs with Internal Adapter)**, with **Option A** as viable alternative

**Why Option B is the recommended choice for Edge:**
1. **Official Support**:
   - **Google**: `google.golang.org/genai` is the official next-gen Google Gen AI Go SDK (supporting Gemini 2.5/2.0/1.5 Flash/Pro).
   - **OpenAI**: `github.com/openai/openai-go` is the official OpenAI Go SDK.
   - **Anthropic**: `github.com/anthropics/anthropic-sdk-go` is the official Anthropic Go SDK.
2. **The "OpenAI-Compatible" Leverage**:
   Crucially, `github.com/openai/openai-go` can target **any OpenAI-compatible endpoint** simply by configuring `option.WithBaseURL(...)`. This single official SDK covers:
   - Direct **OpenAI** (ChatGPT / GPT-4o)
   - **OpenRouter** (`https://openrouter.ai/api/v1`)
   - **llama.cpp server** (`http://localhost:8080/v1`)
   - **Ollama** (`http://localhost:11434/v1` - Ollama provides a native `/v1` OpenAI-compatible endpoint)
   - **LocalAI / vLLM / LiteLLM**
3. **Lightweight Internal Abstraction**:
   Instead of exposing third-party framework types, `monster-mq-edge` defines a clean, thread-safe internal interface in `internal/genai`:
   ```go
   type Provider interface {
       Name() string
       Model() string
       Generate(ctx context.Context, req Request) (*Response, error)
       IsReady() bool
       Close() error
   }
   ```
   Adapters for Gemini (`google.golang.org/genai`), Anthropic (`anthropic-sdk-go`), and OpenAI-compatible (`openai-go`) each require under 80 lines of Go code.
4. **ARM Cross-Compilation & CGO-Free**:
   All three official SDKs are 100% pure Go, compile cleanly for `linux/arm64` and `linux/armv7`, and introduce zero CGO dependencies.

*(Note: If owner prefers using LangChainGo to mirror LangChain4j directly, the internal `Provider` interface remains identical, and only the underlying adapter instantiates `llms.Model` instead).*

---

## 3. Configuration Parity (1:1 with Main Broker)

### 3.1 `config.yaml` Structure

The `config.yaml` layout matches the JVM broker:

```yaml
Features:
  GenAi: true

GenAI:
  Enabled: true
  Assistant:
    Provider: "gemini"               # gemini | claude | openai | ollama | llamacpp | openrouter
    Model: "gemini-2.5-flash"
    Temperature: 0.7
    DocsPath: "docs"
    Endpoint: ""                     # optional custom endpoint
    ServiceVersion: ""              # optional API version
  Providers:
    Gemini:
      ApiKey: "${GEMINI_API_KEY}"
    Claude:
      ApiKey: "${ANTHROPIC_API_KEY}"
    OpenAI:
      ApiKey: "${OPENAI_API_KEY}"
      Endpoint: "http://localhost:8080/v1"
      TimeoutSeconds: 60
    Ollama:
      BaseUrl: "http://localhost:11434"
      TimeoutSeconds: 120
    LlamaCpp:
      Endpoint: "http://localhost:8080/v1"
      Model: "llama-3-8b"
      ApiKey: ""
      TimeoutSeconds: 60
    OpenRouter:
      Type: "chat"                   # chat | decision
      ApiKey: "${OPENROUTER_API_KEY}"
      Model: "anthropic/claude-3.5-sonnet"
      Endpoint: "https://openrouter.ai/api/v1"
      TimeoutSeconds: 60
```

### 3.2 Go Config Structs (`internal/config/config.go`)

```go
type FeaturesConfig struct {
    // ... existing flags ...
    GenAi bool `yaml:"GenAi"`
}

type GenAIConfig struct {
    Enabled   bool                 `yaml:"Enabled"`
    Assistant GenAiAssistantConfig `yaml:"Assistant"`
    Providers GenAiProvidersConfig `yaml:"Providers"`
}

type GenAiAssistantConfig struct {
    Provider       string  `yaml:"Provider"`
    Model          string  `yaml:"Model"`
    Temperature    float64 `yaml:"Temperature"`
    DocsPath       string  `yaml:"DocsPath"`
    Endpoint       string  `yaml:"Endpoint"`
    ServiceVersion string  `yaml:"ServiceVersion"`
}

type GenAiProvidersConfig struct {
    Gemini     GenAiApiKeyConfig       `yaml:"Gemini"`
    Claude     GenAiApiKeyConfig       `yaml:"Claude"`
    OpenAI     GenAiOpenAIConfig       `yaml:"OpenAI"`
    Ollama     GenAiOllamaConfig       `yaml:"Ollama"`
    LlamaCpp   GenAiLlamaCppConfig     `yaml:"LlamaCpp"`
    OpenRouter GenAiOpenRouterConfig   `yaml:"OpenRouter"`
}

type GenAiApiKeyConfig struct {
    ApiKey string `yaml:"ApiKey"`
}

type GenAiOpenAIConfig struct {
    ApiKey         string `yaml:"ApiKey"`
    Endpoint       string `yaml:"Endpoint"`
    TimeoutSeconds int    `yaml:"TimeoutSeconds"`
}

type GenAiOllamaConfig struct {
    BaseUrl        string `yaml:"BaseUrl"`
    TimeoutSeconds int    `yaml:"TimeoutSeconds"`
}

type GenAiLlamaCppConfig struct {
    Endpoint       string `yaml:"Endpoint"`
    Model          string `yaml:"Model"`
    ApiKey         string `yaml:"ApiKey"`
    TimeoutSeconds int    `yaml:"TimeoutSeconds"`
}

type GenAiOpenRouterConfig struct {
    Type           string `yaml:"Type"`
    ApiKey         string `yaml:"ApiKey"`
    Model          string `yaml:"Model"`
    Endpoint       string `yaml:"Endpoint"`
    TimeoutSeconds int    `yaml:"TimeoutSeconds"`
}
```

The root `Config` struct will include:
```go
GenAI GenAIConfig `yaml:"GenAI"`
```
And `yaml-json-schema.json` will be updated to include the exact JSON schema definition from the main broker.

---

## 4. GraphQL Schema Parity (1:1 with Main Broker)

### 4.1 Schema Files to Add

Under `internal/graphql/schema/`:
1. `genai.graphqls` — copied verbatim from `../main/broker/src/main/resources/schema-genai.graphqls`.
2. `genai-providers.graphqls` — copied verbatim from `../main/broker/src/main/resources/schema-genai-providers.graphqls`.
3. Supporting catalog proposal types (needed by `proposeDataCatalog`):
   ```graphql
   type DataCatalogType {
       id: String!
       namespace: String!
       name: String!
       description: String
       structure: JSON!
       topicPattern: String
       createdAt: String
       updatedAt: String
   }

   type DataCatalogInstance {
       id: String!
       typeId: String!
       name: String!
       baseTopic: String!
       properties: JSON!
       createdAt: String
       updatedAt: String
   }

   type DataCatalogRelation {
       sourceId: String!
       targetId: String!
       relationType: String!
   }

   type DataCatalogProposal {
       types: [DataCatalogType!]!
       instances: [DataCatalogInstance!]!
       relations: [DataCatalogRelation!]!
       topicsAnalyzed: Int!
       summary: String
       error: String
   }
   ```

### 4.2 Root Query & Mutation Extensions

```graphql
extend type Query {
    genai: GenAiQuery
    genAiProviders: [GenAiProvider!]!
    genAiProvider(name: String!): GenAiProvider
}

extend type Mutation {
    genAiProvider: GenAiProviderMutations
}
```

---

## 5. Subsystem Design & Package Architecture

### 5.1 Repository Layout

```
internal/
  genai/                   # GenAI core subsystem
    provider.go            # Provider, Request, Response interfaces & structs
    factory.go             # Factory creating Provider from config or DeviceConfig
    adapter_gemini.go      # google.golang.org/genai adapter
    adapter_openai.go      # github.com/openai/openai-go adapter (OpenAI, OpenRouter, llama.cpp, Ollama)
    adapter_claude.go      # github.com/anthropics/anthropic-sdk-go adapter
    tree.go                # Hierarchical ASCII tree formatter for topic analysis
    docs.go                # Markdown documentation loader for RAG context
  graphql/
    schema/
      genai.graphqls       # GenAI GraphQL schema (parity)
      genai-providers.graphqls # Dynamic GenAI provider CRUD schema (parity)
    resolvers/
      genai.go             # Resolvers for GenAiQuery (generate, analyzeTopics, proposeDataCatalog)
      genai_providers.go   # Resolvers for genAiProviders query & mutation
```

### 5.2 Dynamic Provider Store

Like the JVM broker, Edge supports two provider sources:
1. **Config providers**: Read from `config.yaml` (`GenAI.Providers.*`). These are marked with `source: "config"`.
2. **Database providers**: Created via `mutation { genAiProvider { create(...) } }`. Stored in `stores.DeviceConfigStore` with:
   - `Type = "genai_provider"`
   - `Config = JSON string` (`{ "type": "...", "model": "...", "apiKey": "...", "endpoint": "...", "temperature": 0.7, "maxTokens": 4096 }`)
   - Marked with `source: "database"`.

`genAiProviders` query returns config providers first, followed by database providers.

### 5.3 Topic Analysis & Hierarchy Compression (`tree.go`)

In `analyzeTopics` and `proposeDataCatalog`, the resolver fetches topic values matching `topicPattern` from the active `LastValueStore` via `store.FindMatchingMessages(ctx, pattern, ...)`.

To minimize LLM token consumption and improve structural comprehension:
1. Non-UTF8 or binary payloads (> 10 KB or containing control characters) are skipped.
2. Long values are truncated to `maxValueLength` (default 1000 chars).
3. The topic paths are converted into an ASCII tree structure (`├── `, `└── `, `│   `), ported directly from `GenAiResolver.kt:formatTopicsAsHierarchy`.

Example generated context:
```
ProveIt/
└── Enterprise B/
    └── Site1/
        └── node/
            ├── assetid: 1
            └── name: Site1
```

### 5.4 Feature Gating & Broker EnabledFeatures

1. In `internal/graphql/resolvers/resolver.go`:
   ```go
   func (r *Resolver) enabledFeatures() []string {
       // ...
       if r.Cfg.Features.GenAi || r.Cfg.GenAI.Enabled {
           out = append(out, "GenAi")
       }
       return out
   }
   ```
2. When `Features.GenAi` is false or GenAI is unconfigured:
   - `Query.genai` returns `nil`.
   - `Query.genAiProviders` returns an empty array.
   - `Query.genAiProvider(name)` returns `nil`.
   - Dashboard checks (`hasQueryField('genai')`) cleanly evaluate to `false` without errors.

---

## 6. Implementation Steps

### Phase 1: Configuration & GraphQL Schema Setup
1. Add `GenAi: bool` to `FeaturesConfig` in `internal/config/config.go`.
2. Add `GenAIConfig` and provider sub-structs to `internal/config/config.go`.
3. Add `GenAI` and `Features.GenAi` definitions to `yaml-json-schema.json`, `config.yaml.example`, and `scripts/deb/config.yaml`.
4. Copy `schema-genai.graphqls` and `schema-genai-providers.graphqls` into `internal/graphql/schema/`.
5. Add catalog proposal types required by `proposeDataCatalog`.
6. Run `make gen` (`gqlgen`) to generate Go models and resolver interfaces.

### Phase 2: Core `internal/genai` Package
1. Define `genai.Provider`, `genai.Request`, and `genai.Response`.
2. Implement `adapter_openai.go` using `github.com/openai/openai-go` (handles OpenAI, OpenRouter, llama.cpp, and Ollama).
3. Implement `adapter_gemini.go` using `google.golang.org/genai`.
4. Implement `adapter_claude.go` using `github.com/anthropics/anthropic-sdk-go`.
5. Implement `tree.go` (topic tree builder and ASCII tree renderer).
6. Implement `docs.go` (local markdown file reader with path traversal protection).
7. Implement `factory.go` to construct providers from YAML config or `DeviceConfig`.

### Phase 3: Resolvers & Store Integration
1. Implement `genAiProviders` and `genAiProvider` queries in `internal/graphql/resolvers/genai_providers.go`.
2. Implement `GenAiProviderMutations` (`create`, `update`, `delete`) in `internal/graphql/resolvers/genai_providers.go` using `r.Stores.DeviceConfigStore`.
3. Implement `Query.genai` resolver returning `GenAiQuery`.
4. Implement `GenAiQuery.generate` resolver with optional doc reading and prompt context.
5. Implement `GenAiQuery.analyzeTopics` resolver with `LastValueStore` matching and chat history.
6. Implement `GenAiQuery.proposeDataCatalog` resolver with prompt framing and JSON extraction.
7. Update `r.enabledFeatures()` in `resolver.go`.

### Phase 4: Testing & Verification
1. **Unit Tests**:
   - `tree_test.go`: ASCII tree formatting logic with various topic hierarchy depths.
   - `docs_test.go`: Markdown reader and path boundary checks.
   - `factory_test.go`: Provider instantiation from mock YAML and DB configs.
2. **Integration Tests (`test/integration/genai_test.go`)**:
   - Use an HTTP mock server simulating OpenAI/Ollama endpoints.
   - Test `generate`, `analyzeTopics`, and `proposeDataCatalog` end-to-end via GraphQL query execution.
   - Test `genAiProvider` CRUD mutations and persistence in SQLite / Postgres / MongoDB.
   - Test feature flag disablement (returns `nil`/empty when `Features.GenAi: false`).
3. **Cross-Broker Parity Check**:
   - Run the MonsterMQ Dashboard against the edge broker.
   - Verify that the AI card appears in Script Editor and Topic Browser.
   - Verify that AI Providers page lists both config and DB providers.
4. **Build Verification**:
   - Verify `make build` (CGO_ENABLED=0), `make build-arm64`, `make build-armv7`, `make test`, and `make lint`.

---

## 7. Open Questions & Sign-Off

1. **SDK Preference**: Owner confirmation on using official SDKs (`openai-go`, `google.golang.org/genai`, `anthropic-sdk-go`) vs LangChainGo (`github.com/tmc/langchaingo`).
2. **Local Edge Default**: Should Ollama or llama.cpp be recommended in `config.yaml.example` as the default edge provider when deploying offline?
3. **Data Catalog CRUD**: Confirm that `proposeDataCatalog` generates the proposal type, while the full Data Catalog persistence CRUD (`dataCatalogTypes`, `saveType`, etc.) remains a separate future plan.

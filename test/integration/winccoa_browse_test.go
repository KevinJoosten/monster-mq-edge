package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/oahost"
)

// startNativeGraphQL starts an embedded broker on the simulated host with
// the GraphQL endpoint enabled.
func startNativeGraphQL(t *testing.T, mqttPort, gqlPort int, cfgFn func(*config.Config)) (*nativeEnv, string) {
	t.Helper()
	sim, client := newSim(0)
	t.Cleanup(sim.Close)
	env := startNative(t, mqttPort, filepath.Join(t.TempDir(), "b.db"), sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = gqlPort
		c.GraphQL.TLSPort = gqlPort + 1000
		if cfgFn != nil {
			cfgFn(c)
		}
	}, broker.Options{})
	t.Cleanup(func() { env.srv.Close() })
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", gqlPort)); err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return env, fmt.Sprintf("http://localhost:%d/graphql", gqlPort)
}

// AC-02: Query.winccoa is null standalone and non-null embedded.
func TestWinCCOaBrowseStandaloneNull(t *testing.T) {
	srv, url := startWithGraphQL(t, 27610, 28610)
	defer srv.Close()
	data := gqlQuery(t, url, `{ winccoa { systems { name } } }`, nil)
	if data["winccoa"] != nil {
		t.Fatalf("winccoa = %v, want null", data["winccoa"])
	}
}

// AC-02..AC-05 against the simulated host.
func TestWinCCOaBrowseGraphQL(t *testing.T) {
	env, url := startNativeGraphQL(t, 27611, 28611, nil)
	if err := env.sim.SetMeta("System1:Pump101.speed", "rpm", "%.0f", "Motor speed"); err != nil {
		t.Fatal(err)
	}
	if err := env.sim.Set("System1:Pump101.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 1450}); err != nil {
		t.Fatal(err)
	}
	q := func(query string) map[string]any {
		return gqlQuery(t, url, query, nil)["winccoa"].(map[string]any)
	}

	systems := q(`{ winccoa { systems { name local connected } } }`)["systems"].([]any)
	got := map[string]string{}
	for _, s := range systems {
		m := s.(map[string]any)
		got[m["name"].(string)] = fmt.Sprintf("%v/%v", m["local"], m["connected"])
	}
	if got["System1"] != "true/true" || got["SubstationA"] != "false/true" || got["SubstationB"] != "" {
		t.Fatalf("systems %v", got)
	}

	types := q(`{ winccoa { types(pattern: "*") } }`)["types"].([]any)
	for _, ty := range types {
		if s := ty.(string); s == "MMQConfigs" || s[0] == '_' {
			t.Fatalf("protected type listed: %v", types)
		}
	}
	ts := q(`{ winccoa { typeStructure(type: "AnalogDrive") { type elements { path name depth elementType valueElement } } } }`)["typeStructure"].(map[string]any)
	els := ts["elements"].([]any)
	if len(els) != 11 || els[0].(map[string]any)["path"] != "" || els[0].(map[string]any)["elementType"] != "struct" {
		t.Fatalf("structure %v", els)
	}
	unknown := gqlRequest(t, url, `{ winccoa { typeStructure(type: "Nope") { type } } }`, nil, "")
	if !graphqlErrorContains(unknown, "not found") {
		t.Fatalf("unknown type: %v", unknown)
	}

	page := q(`{ winccoa { datapoints(pattern: "*", limit: 2, offset: 0) { total limit items { name system type } } } }`)["datapoints"].(map[string]any)
	if page["total"].(float64) != 3 || len(page["items"].([]any)) != 2 {
		t.Fatalf("page %v", page)
	}
	page = q(`{ winccoa { datapoints(limit: 99999) { limit } } }`)["datapoints"].(map[string]any)
	if page["limit"].(float64) != 1000 {
		t.Fatalf("limit not capped: %v", page)
	}
	page = q(`{ winccoa { datapoints(system: "SubstationA") { total items { name system } } } }`)["datapoints"].(map[string]any)
	if page["total"].(float64) != 2 {
		t.Fatalf("remote page %v", page)
	}
	down := gqlRequest(t, url, `{ winccoa { datapoints(system: "SubstationB") { total } } }`, nil, "")
	if !graphqlErrorContains(down, "not available") {
		t.Fatalf("disconnected system: %v", down)
	}

	elements := q(`{ winccoa { elements(dp: "Pump101") { dpe path elementType } } }`)["elements"].([]any)
	if len(elements) != 11 {
		t.Fatalf("elements %v", elements)
	}
	infos := q(`{ winccoa { elementInfo(dpes: ["Pump101.speed", "Pump101.nope"]) { dpe exists error type elementType unit format description value stime status } } }`)["elementInfo"].([]any)
	speed, missing := infos[0].(map[string]any), infos[1].(map[string]any)
	var v float64
	_ = json.Unmarshal([]byte(speed["value"].(string)), &v)
	if speed["exists"] != true || speed["unit"] != "rpm" || speed["description"] != "Motor speed" || v != 1450 || speed["type"] != "AnalogDrive" || speed["stime"] == nil {
		t.Fatalf("speed %v", speed)
	}
	if missing["exists"] != false || missing["error"] == nil {
		t.Fatalf("missing %v", missing)
	}
	many := make([]string, 201)
	for i := range many {
		many[i] = "Pump101.speed"
	}
	tooMany := gqlRequest(t, url, `query($d: [String!]!) { winccoa { elementInfo(dpes: $d) { dpe } } }`, map[string]any{"d": many}, "")
	if !graphqlErrorContains(tooMany, "at most 200") {
		t.Fatalf("201 DPEs: %v", tooMany)
	}
}

// AC-02: unauthenticated requests are rejected; a user without read
// permission on the native branch is denied.
func TestWinCCOaBrowseAuth(t *testing.T) {
	_, url := startNativeGraphQL(t, 27612, 28612, func(c *config.Config) {
		c.UserManagement.Enabled = true
		c.UserManagement.AnonymousEnabled = false
	})
	if r := gqlRequest(t, url, `{ winccoa { systems { name } } }`, nil, ""); !graphqlErrorContains(r, "authentication required") {
		t.Fatalf("unauthenticated: %v", r)
	}
	admin := loginToken(t, url, "Admin", "Admin")
	gqlQueryAuth(t, url, `mutation { user { createUser(input: { username: "op", password: "pw", canSubscribe: true }) { success } } }`, nil, admin)
	gqlQueryAuth(t, url, `mutation { user { createAclRule(input: { username: "op", topicPattern: "plant/#", canSubscribe: true, canPublish: false, priority: 1 }) { success } } }`, nil, admin)
	op := loginToken(t, url, "op", "pw")
	if r := gqlRequest(t, url, `{ winccoa { systems { name } } }`, nil, op); !graphqlErrorContains(r, "permission denied") {
		t.Fatalf("op without ACL: %v", r)
	}
	if data := gqlQueryAuth(t, url, `{ winccoa { systems { name } } }`, nil, admin); data["winccoa"] == nil {
		t.Fatal("admin denied")
	}
}

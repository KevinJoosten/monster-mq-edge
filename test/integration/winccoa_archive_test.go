package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/broker"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/oahost"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/stores/oastore"
)

func TestWinCCOaArchiveGroupProvisioned(t *testing.T) {
	env, url := startNativeGraphQL(t, 27701, 28701, nil)
	_ = env

	data := gqlQuery(t, url, `{ archiveGroup(name: "WinCCOA") { name enabled deployed topicFilter lastValType archiveType payloadFormat lastValReadOnly archiveReadOnly } }`, nil)
	ag, ok := data["archiveGroup"].(map[string]any)
	if !ok || ag == nil {
		t.Fatalf("expected WinCCOA archiveGroup, got %v", data)
	}
	if ag["name"] != "WinCCOA" || ag["enabled"] != true || ag["deployed"] != true {
		t.Fatalf("unexpected status: %v", ag)
	}
	if ag["lastValType"] != "WINCCOA" || ag["archiveType"] != "WINCCOA" {
		t.Fatalf("unexpected store types: %v", ag)
	}
	if ag["lastValReadOnly"] != true || ag["archiveReadOnly"] != true {
		t.Fatalf("expected read-only flags true, got %v", ag)
	}
	tf, _ := ag["topicFilter"].([]any)
	if len(tf) != 1 || tf[0] != "winccoa/#" {
		t.Fatalf("expected topicFilter [winccoa/#], got %v", tf)
	}

	// Custom TopicRoot provisions [<root>/#].
	_, urlCustom := startNativeGraphQL(t, 27702, 28702, func(c *config.Config) {
		c.WinCCOaNative.TopicRoot = "plant/oa"
	})
	dataCustom := gqlQuery(t, urlCustom, `{ archiveGroup(name: "WinCCOA") { topicFilter } }`, nil)
	agCustom := dataCustom["archiveGroup"].(map[string]any)
	tfCustom, _ := agCustom["topicFilter"].([]any)
	if len(tfCustom) != 1 || tfCustom[0] != "plant/oa/#" {
		t.Fatalf("expected topicFilter [plant/oa/#], got %v", tfCustom)
	}
}

func TestWinCCOaArchiveGroupNotProvisionedWithoutNamespace(t *testing.T) {
	_, url := startNativeGraphQL(t, 27703, 28703, func(c *config.Config) {
		c.WinCCOaNative.Namespace = false
	})
	data := gqlQuery(t, url, `{ archiveGroup(name: "WinCCOA") { name } }`, nil)
	if data["archiveGroup"] != nil {
		t.Fatalf("expected nil WinCCOA group when Namespace=false, got %v", data["archiveGroup"])
	}
}

func TestWinCCOaArchiveGroupRecreatedOnRestart(t *testing.T) {
	sim, client := newSim(0)
	defer sim.Close()
	dbPath := filepath.Join(t.TempDir(), "restart.db")

	env1 := startNative(t, 27704, dbPath, sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = 28704
		c.GraphQL.TLSPort = 29704
	}, broker.Options{})
	url1 := "http://localhost:28704/graphql"
	waitForHealth(t, 28704)

	delRes := gqlQuery(t, url1, `mutation { archiveGroup { delete(name: "WinCCOA") { success } } }`, nil)
	if delRes["archiveGroup"].(map[string]any)["delete"].(map[string]any)["success"] != true {
		t.Fatalf("delete failed: %v", delRes)
	}
	// Reload does not recreate a deleted WinCCOA group during the same process lifetime.
	if err := env1.srv.Archives().Reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	afterReload := gqlQuery(t, url1, `{ archiveGroup(name: "WinCCOA") { name } }`, nil)
	if afterReload["archiveGroup"] != nil {
		t.Fatalf("Reload recreated WinCCOA group: %v", afterReload)
	}
	env1.srv.Close()

	// Next startup Load recreates WinCCOA.
	env2 := startNative(t, 27705, dbPath, sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = 28705
		c.GraphQL.TLSPort = 29705
	}, broker.Options{})
	defer env2.srv.Close()
	url2 := "http://localhost:28705/graphql"
	waitForHealth(t, 28705)

	afterRestart := gqlQuery(t, url2, `{ archiveGroup(name: "WinCCOA") { name deployed } }`, nil)
	ag, _ := afterRestart["archiveGroup"].(map[string]any)
	if ag == nil || ag["deployed"] != true {
		t.Fatalf("expected recreated WinCCOA group after restart, got %v", afterRestart)
	}
}

func TestWinCCOaArchiveGroupDisablePersists(t *testing.T) {
	sim, client := newSim(0)
	defer sim.Close()
	dbPath := filepath.Join(t.TempDir(), "disable.db")

	env1 := startNative(t, 27706, dbPath, sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = 28706
		c.GraphQL.TLSPort = 29706
	}, broker.Options{})
	url1 := "http://localhost:28706/graphql"
	waitForHealth(t, 28706)

	disRes := gqlQuery(t, url1, `mutation { archiveGroup { disable(name: "WinCCOA") { success archiveGroup { enabled deployed } } } }`, nil)
	disGroup := disRes["archiveGroup"].(map[string]any)["disable"].(map[string]any)["archiveGroup"].(map[string]any)
	if disGroup["enabled"] != false || disGroup["deployed"] != false {
		t.Fatalf("expected disabled group, got %v", disGroup)
	}
	env1.srv.Close()

	env2 := startNative(t, 27707, dbPath, sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = 28707
		c.GraphQL.TLSPort = 29707
	}, broker.Options{})
	defer env2.srv.Close()
	url2 := "http://localhost:28707/graphql"
	waitForHealth(t, 28707)

	afterRestart := gqlQuery(t, url2, `{ archiveGroup(name: "WinCCOA") { enabled deployed } }`, nil)
	ag := afterRestart["archiveGroup"].(map[string]any)
	if ag["enabled"] != false || ag["deployed"] != false {
		t.Fatalf("Disable did not persist across restart: %v", ag)
	}
}

func TestWinCCOaArchiveStandaloneDeployError(t *testing.T) {
	srv, url := startWithGraphQL(t, 27708, 28708)
	defer srv.Close()

	createRes := gqlQuery(t, url, `mutation {
		archiveGroup {
			create(input: {
				name: "BadOA"
				topicFilter: ["winccoa/#"]
				lastValType: WINCCOA
				archiveType: WINCCOA
			}) {
				success
				message
			}
		}
	}`, nil)
	cr := createRes["archiveGroup"].(map[string]any)["create"].(map[string]any)
	if cr["success"] != false || !strings.Contains(fmt.Sprint(cr["message"]), archive.WinCCOAUnavailableMessage) {
		t.Fatalf("expected standalone create rejection, got %v", cr)
	}

	updRes := gqlQuery(t, url, `mutation {
		archiveGroup {
			update(input: {
				name: "Default"
				lastValType: WINCCOA
			}) {
				success
				message
			}
		}
	}`, nil)
	ur := updRes["archiveGroup"].(map[string]any)["update"].(map[string]any)
	if ur["success"] != false || !strings.Contains(fmt.Sprint(ur["message"]), archive.WinCCOAUnavailableMessage) {
		t.Fatalf("expected standalone update rejection, got %v", ur)
	}

	// Pre-seed a WINCCOA row directly in ArchiveConfig and reload.
	ctx := context.Background()
	if err := srv.Storage().ArchiveConfig.Save(ctx, archive.WinCCOAGroupConfig("winccoa")); err != nil {
		t.Fatalf("seed WinCCOA config: %v", err)
	}
	if err := srv.Archives().Reload(ctx); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if deployErr := srv.Archives().DeployError("WinCCOA"); !strings.Contains(deployErr, archive.WinCCOAUnavailableMessage) {
		t.Fatalf("expected DeployError with unavailable message, got %q", deployErr)
	}
	// Default group remains deployed.
	if srv.Archives().Get("Default") == nil {
		t.Fatal("Default group should stay deployed")
	}
}

func TestWinCCOaArchiveOldHost(t *testing.T) {
	sim, client := newSim(0)
	defer sim.Close()
	sim.DisableArchiveOps = true

	env := startNative(t, 27709, filepath.Join(t.TempDir(), "oldhost.db"), sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = 28709
		c.GraphQL.TLSPort = 29709
	}, broker.Options{})
	defer env.srv.Close()
	url := "http://localhost:28709/graphql"
	waitForHealth(t, 28709)

	deployErr := env.srv.Archives().DeployError("WinCCOA")
	if !strings.Contains(deployErr, "WCCOAmmq does not support archive reads") {
		t.Fatalf("expected old-host deploy error, got %q", deployErr)
	}
	data := gqlQuery(t, url, `{
		oa: archiveGroup(name: "WinCCOA") { deployed }
		def: archiveGroup(name: "Default") { deployed }
	}`, nil)
	if data["oa"].(map[string]any)["deployed"] != false {
		t.Fatalf("expected WinCCOA deployed=false, got %v", data["oa"])
	}
	if data["def"].(map[string]any)["deployed"] != true {
		t.Fatalf("expected Default deployed=true, got %v", data["def"])
	}
}

func TestWinCCOaArchiveReadOnlyForced(t *testing.T) {
	env, url := startNativeGraphQL(t, 27710, 28710, nil)
	if err := env.sim.Set("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 42}); err != nil {
		t.Fatal(err)
	}

	upd := gqlQuery(t, url, `mutation {
		archiveGroup {
			update(input: {
				name: "WinCCOA"
				lastValReadOnly: false
				archiveReadOnly: false
			}) {
				success
				archiveGroup { lastValReadOnly archiveReadOnly deployed }
			}
		}
	}`, nil)
	ag := upd["archiveGroup"].(map[string]any)["update"].(map[string]any)["archiveGroup"].(map[string]any)
	if ag["lastValReadOnly"] != true || ag["archiveReadOnly"] != true || ag["deployed"] != true {
		t.Fatalf("expected forced read-only flags, got %v", ag)
	}

	// Reload does not restart the group (same group pointer).
	gBefore := env.srv.Archives().Get("WinCCOA")
	if err := env.srv.Archives().Reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}
	gAfter := env.srv.Archives().Get("WinCCOA")
	if gBefore != gAfter {
		t.Fatal("Reload unexpectedly restarted WinCCOA group")
	}

	// Publishing to a native value topic does not write to WinCC OA via the archive group.
	setsBefore := env.sim.Sets()
	pubClient, _ := dialRaw(t, 27710, rawConnect{ClientID: "ro-pub", Version: 5, Clean: true})
	defer pubClient.Close()
	pubClient.Publish(rawPub{Topic: "winccoa/tags/Pump1/speed", Payload: []byte(`{"value":999}`), QoS: 0})
	time.Sleep(150 * time.Millisecond)
	if env.sim.Sets() != setsBefore {
		t.Fatalf("expected no WinCC OA sets from value-topic publish, got %d -> %d", setsBefore, env.sim.Sets())
	}

	// Mixed group: LastValType=WINCCOA, ArchiveType=SQLITE, LastValReadOnly=false.
	mixRes := gqlQuery(t, url, `mutation {
		archiveGroup {
			create(input: {
				name: "MixedOA"
				topicFilter: ["winccoa/#", "mixed/#"]
				lastValType: WINCCOA
				archiveType: SQLITE
				lastValReadOnly: false
				archiveReadOnly: false
				bulkTimeoutMs: 50
			}) {
				success
				message
				archiveGroup { lastValReadOnly archiveReadOnly deployed }
			}
		}
	}`, nil)
	mc := mixRes["archiveGroup"].(map[string]any)["create"].(map[string]any)
	if mc["success"] != true {
		t.Fatalf("mixed group create failed: %v", mc)
	}
	mag := mc["archiveGroup"].(map[string]any)
	if mag["lastValReadOnly"] != true || mag["archiveReadOnly"] != false {
		t.Fatalf("unexpected mixed group read-only flags: %v", mag)
	}
	// Publish on mixed/test gets archived into MixedOA's SQLite archive, while currentValue for Pump1/speed comes from WinCC OA.
	pubClient.Publish(rawPub{Topic: "mixed/test", Payload: []byte(`{"hello":"world"}`), QoS: 0})
	time.Sleep(250 * time.Millisecond)
	histData := gqlQuery(t, url, `{
		archivedMessages(topicFilter: "mixed/test", archiveGroup: "MixedOA") { payload }
		currentValue(topic: "winccoa/tags/Pump1/speed", archiveGroup: "MixedOA") { payload }
	}`, nil)
	msgs := histData["archivedMessages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 SQLite archived message in MixedOA, got %v", msgs)
	}
	cv := histData["currentValue"].(map[string]any)
	if !strings.Contains(fmt.Sprint(cv["payload"]), "42") {
		t.Fatalf("expected currentValue from WinCC OA in MixedOA, got %v", cv)
	}
}

func TestWinCCOaArchiveGraphQLEnum(t *testing.T) {
	_, url := startNativeGraphQL(t, 27711, 28711, nil)

	data := gqlQuery(t, url, `{
		eq: archiveGroups(lastValTypeEquals: WINCCOA) { name lastValType archiveType }
		neq: archiveGroups(lastValTypeNotEquals: NONE) { name lastValType }
	}`, nil)
	eq := data["eq"].([]any)
	if len(eq) != 1 || eq[0].(map[string]any)["name"] != "WinCCOA" {
		t.Fatalf("lastValTypeEquals WINCCOA = %v", eq)
	}
	neq := data["neq"].([]any)
	found := false
	for _, item := range neq {
		m := item.(map[string]any)
		if m["name"] == "WinCCOA" && m["lastValType"] == "WINCCOA" {
			found = true
		}
	}
	if !found {
		t.Fatalf("WinCCOA missing from lastValTypeNotEquals NONE: %v", neq)
	}
}

func TestWinCCOaArchiveCurrentValueExact(t *testing.T) {
	env, url := startNativeGraphQL(t, 27712, 28712, nil)
	ts := time.Date(2026, 10, 17, 12, 0, 0, 123000000, time.UTC)
	if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 1480.5}, ts); err != nil {
		t.Fatal(err)
	}
	if err := env.sim.SetTimed("System1:ScalarTag.:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 7.25}, ts); err != nil {
		t.Fatal(err)
	}
	if err := env.sim.SetTimed("SubstationA:Feeder1.voltage:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 230.0}, ts); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		topic   string
		wantVal any
	}{
		{"winccoa/tags/Pump1/speed", 1480.5},
		{"winccoa/tags/ScalarTag", 7.25},
		{"winccoa/tags/Pump1/speed/_online.._stime", "2026-10-17T12:00:00.123Z"},
		{"winccoa/systems/SubstationA/tags/Feeder1/voltage", 230.0},
		{"winccoa/types/AnalogDrive/Pump1/speed", 1480.5},
	}

	for _, tc := range cases {
		res := gqlQuery(t, url, fmt.Sprintf(`{ currentValue(topic: %q, archiveGroup: "WinCCOA") { topic payload timestamp } }`, tc.topic), nil)
		cv, ok := res["currentValue"].(map[string]any)
		if !ok || cv == nil {
			t.Fatalf("%s: expected non-nil currentValue, got %v", tc.topic, res)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(cv["payload"].(string)), &payload); err != nil {
			t.Fatalf("%s: invalid JSON payload %q: %v", tc.topic, cv["payload"], err)
		}
		if payload["value"] != tc.wantVal || payload["time"] != "2026-10-17T12:00:00.123Z" {
			t.Fatalf("%s: unexpected payload %v", tc.topic, payload)
		}
		if cv["timestamp"] != float64(ts.UnixMilli()) {
			t.Fatalf("%s: timestamp = %v, want %d", tc.topic, cv["timestamp"], ts.UnixMilli())
		}
	}
}

func TestWinCCOaArchiveCurrentValueRejected(t *testing.T) {
	_, url := startNativeGraphQL(t, 27713, 28713, nil)
	rejected := []string{
		"winccoa/tags/Pump1/speed/set",
		"winccoa/tags/_Users",
		"winccoa/tags/MMQConfigs_k1",
		"winccoa/tags/Pump1/nested",
		"winccoa/types/Feeder/Pump1/speed",
		"winccoa/topics/foo",
		"winccoa/systems/System1",
		"sensors/temp",
	}
	for _, topic := range rejected {
		res := gqlQuery(t, url, fmt.Sprintf(`{ currentValue(topic: %q, archiveGroup: "WinCCOA") { topic payload } }`, topic), nil)
		if res["currentValue"] != nil {
			t.Fatalf("topic %q: expected nil currentValue, got %v", topic, res["currentValue"])
		}
	}
}

func TestWinCCOaArchiveRemoteUnavailable(t *testing.T) {
	env, url := startNativeGraphQL(t, 27714, 28714, nil)
	topic := "winccoa/systems/SubstationB/tags/Pump1/speed"

	store := env.srv.Archives().Get("WinCCOA").LastValue()
	_, err := store.Get(context.Background(), topic)
	if !errors.Is(err, oahost.ErrUnavailable) {
		t.Fatalf("store.Get(%q) err = %v, want errors.Is ErrUnavailable", topic, err)
	}

	raw := gqlRequest(t, url, fmt.Sprintf(`{ currentValue(topic: %q, archiveGroup: "WinCCOA") { payload } }`, topic), nil, "")
	if !graphqlErrorContains(raw, oahost.ErrUnavailable.Error()) {
		t.Fatalf("expected GraphQL error containing %q, got %v", oahost.ErrUnavailable.Error(), raw)
	}
}

func TestWinCCOaArchiveCurrentValuesScoped(t *testing.T) {
	env, url := startNativeGraphQL(t, 27715, 28715, nil)
	if err := env.sim.Set("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 11}); err != nil {
		t.Fatal(err)
	}
	if err := env.sim.Set("System1:Pump101.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 22}); err != nil {
		t.Fatal(err)
	}
	if err := env.sim.Set("SubstationA:Feeder1.voltage:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 33}); err != nil {
		t.Fatal(err)
	}

	topicsOf := func(filter string) []string {
		res := gqlQuery(t, url, fmt.Sprintf(`{ currentValues(topicFilter: %q, archiveGroup: "WinCCOA", limit: 100) { topic } }`, filter), nil)
		list := res["currentValues"].([]any)
		out := make([]string, 0, len(list))
		for _, item := range list {
			out = append(out, item.(map[string]any)["topic"].(string))
		}
		sort.Strings(out)
		return out
	}

	pump1 := topicsOf("winccoa/tags/Pump1/#")
	if len(pump1) != 9 {
		t.Fatalf("winccoa/tags/Pump1/# returned %d topics (%v), want 9", len(pump1), pump1)
	}

	speeds := topicsOf("winccoa/tags/+/speed")
	if strings.Join(speeds, ",") != "winccoa/tags/Pump1/speed,winccoa/tags/Pump101/speed" {
		t.Fatalf("winccoa/tags/+/speed = %v", speeds)
	}

	analog := topicsOf("winccoa/types/AnalogDrive/#")
	if len(analog) != 18 {
		t.Fatalf("winccoa/types/AnalogDrive/# returned %d topics, want 18", len(analog))
	}

	anyTypeSpeed := topicsOf("winccoa/types/+/Pump1/speed")
	if strings.Join(anyTypeSpeed, ",") != "winccoa/types/AnalogDrive/Pump1/speed" {
		t.Fatalf("winccoa/types/+/Pump1/speed = %v", anyTypeSpeed)
	}

	subA := topicsOf("winccoa/systems/SubstationA/tags/#")
	if len(subA) != 10 { // Feeder1.voltage (1) + Pump101 (9)
		t.Fatalf("winccoa/systems/SubstationA/tags/# returned %d topics (%v), want 10", len(subA), subA)
	}
	queries := env.sim.QueryLog()
	foundRemote := false
	for _, q := range queries {
		if strings.Contains(q, "REMOTE 'SubstationA'") {
			foundRemote = true
			break
		}
	}
	if !foundRemote {
		t.Fatalf("expected query with REMOTE 'SubstationA', got %v", queries)
	}
}

func TestWinCCOaArchiveBroadFilters(t *testing.T) {
	env, url := startNativeGraphQL(t, 27716, 28716, nil)

	topicsOf := func(filter string) []string {
		res := gqlQuery(t, url, fmt.Sprintf(`{ currentValues(topicFilter: %q, archiveGroup: "WinCCOA", limit: 100) { topic } }`, filter), nil)
		list := res["currentValues"].([]any)
		out := make([]string, 0, len(list))
		for _, item := range list {
			out = append(out, item.(map[string]any)["topic"].(string))
		}
		sort.Strings(out)
		return out
	}

	// # and winccoa/# return local shortcut tags (Pump1: 9, Pump101: 9, ScalarTag: 1 = 19).
	allHash := topicsOf("#")
	allRootHash := topicsOf("winccoa/#")
	if len(allHash) != 19 || strings.Join(allHash, ",") != strings.Join(allRootHash, ",") {
		t.Fatalf("# (%d) and winccoa/# (%d) mismatch: %v", len(allHash), len(allRootHash), allHash)
	}

	// winccoa/+ has no leaf values and makes 0 OpDpQuery calls.
	qBefore := env.sim.Calls(oahost.OpDpQuery)
	plusOne := topicsOf("winccoa/+")
	qAfter := env.sim.Calls(oahost.OpDpQuery)
	if len(plusOne) != 0 || qAfter != qBefore {
		t.Fatalf("winccoa/+ = %v, OpDpQuery calls %d -> %d", plusOne, qBefore, qAfter)
	}

	// winccoa/+/+ returns scalar roots (winccoa/tags/ScalarTag).
	plusTwo := topicsOf("winccoa/+/+")
	if strings.Join(plusTwo, ",") != "winccoa/tags/ScalarTag" {
		t.Fatalf("winccoa/+/+ = %v, want [winccoa/tags/ScalarTag]", plusTwo)
	}

	// winccoa/systems/+/tags/# returns System1 (19) + SubstationA (10) = 29 topics.
	allSys := topicsOf("winccoa/systems/+/tags/#")
	if len(allSys) != 29 {
		t.Fatalf("winccoa/systems/+/tags/# returned %d topics (%v), want 29", len(allSys), allSys)
	}
}

func TestWinCCOaArchiveStoreQueryMaxRows(t *testing.T) {
	sim, client := newSim(0)
	defer sim.Close()
	for i := 0; i < 300; i++ {
		if err := sim.CreateDP("System1", fmt.Sprintf("BulkScalar_%03d", i), "Scalar"); err != nil {
			t.Fatal(err)
		}
	}

	env := startNative(t, 27717, filepath.Join(t.TempDir(), "maxrows.db"), sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = 28717
		c.GraphQL.TLSPort = 29717
		c.WinCCOaNative.QueryMaxRows = 100
	}, broker.Options{})
	defer env.srv.Close()
	url := "http://localhost:28717/graphql"
	waitForHealth(t, 28717)

	res := gqlQuery(t, url, `{ currentValues(topicFilter: "winccoa/tags/#", archiveGroup: "WinCCOA", limit: 500) { topic } }`, nil)
	list := res["currentValues"].([]any)
	if len(list) != 100 {
		t.Fatalf("expected 100 rows capped by QueryMaxRows=100, got %d", len(list))
	}
}

func TestWinCCOaArchiveHistoryExact(t *testing.T) {
	env, url := startNativeGraphQL(t, 27718, 28718, nil)
	// Clear the default zero sample created by CreateDP, then re-enable archiving.
	if err := env.sim.SetArchived("System1:Pump1.speed", false); err != nil {
		t.Fatal(err)
	}
	if err := env.sim.SetArchived("System1:Pump1.speed", true); err != nil {
		t.Fatal(err)
	}
	// Add a sample older than 24h to verify the default 24h window excludes it when start/end are omitted.
	if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 999}, time.Now().UTC().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Second).Add(-1 * time.Hour)
	for i := 0; i < 10; i++ {
		if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: float64(100 + i)}, base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}

	// Default 24h window with limit=4 returns newest 4 samples in descending order.
	res := gqlQuery(t, url, `{
		archivedMessages(topicFilter: "winccoa/tags/Pump1/speed", archiveGroup: "WinCCOA", limit: 4) {
			topic payload timestamp qos clientId
		}
	}`, nil)
	rows := res["archivedMessages"].([]any)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d (%v)", len(rows), rows)
	}
	for i, r := range rows {
		m := r.(map[string]any)
		if m["qos"] != float64(1) {
			t.Fatalf("row %d metadata: %v", i, m)
		}
		wantIdx := 9 - i
		wantTime := base.Add(time.Duration(wantIdx) * time.Minute)
		if m["timestamp"] != float64(wantTime.UnixMilli()) {
			t.Fatalf("row %d timestamp = %v, want %d", i, m["timestamp"], wantTime.UnixMilli())
		}
		var p map[string]any
		if err := json.Unmarshal([]byte(m["payload"].(string)), &p); err != nil {
			t.Fatalf("row %d payload JSON: %v", i, err)
		}
		if p["value"] != float64(100+wantIdx) || p["time"] == nil {
			t.Fatalf("row %d payload = %v", i, p)
		}
	}

	// Default 24h window with limit=100 excludes the 48h-old sample (returns 10, not 11).
	resAll := gqlQuery(t, url, `{
		archivedMessages(topicFilter: "winccoa/tags/Pump1/speed", archiveGroup: "WinCCOA", limit: 100) { timestamp }
	}`, nil)
	if got := len(resAll["archivedMessages"].([]any)); got != 10 {
		t.Fatalf("expected 10 rows in default 24h window, got %d", got)
	}
}

func TestWinCCOaArchiveHistoryBatches(t *testing.T) {
	sim, client := newSim(0)
	defer sim.Close()
	base := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Minute)
	for i := 0; i < 150; i++ {
		dp := fmt.Sprintf("BatchDP_%03d", i)
		if err := sim.CreateDP("System1", dp, "Scalar"); err != nil {
			t.Fatal(err)
		}
		dpe := "System1:" + dp + ".:_original.._value"
		if err := sim.SetTimed(dpe, oahost.Value{Kind: oahost.KindFloat, Float: float64(i)}, base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	env := startNative(t, 27719, filepath.Join(t.TempDir(), "batches.db"), sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = 28719
		c.GraphQL.TLSPort = 29719
	}, broker.Options{})
	defer env.srv.Close()
	url := "http://localhost:28719/graphql"
	waitForHealth(t, 28719)

	before := sim.Calls(oahost.OpDpGetPeriod)
	res := gqlQuery(t, url, `{
		archivedMessages(topicFilter: "winccoa/types/Scalar/#", archiveGroup: "WinCCOA", limit: 5) {
			topic timestamp
		}
	}`, nil)
	after := sim.Calls(oahost.OpDpGetPeriod)
	if after-before != 2 {
		t.Fatalf("expected 2 OpDpGetPeriod batch calls for 151 Scalar DPEs, got %d", after-before)
	}
	rows := res["archivedMessages"].([]any)
	if len(rows) != 5 {
		t.Fatalf("expected 5 newest rows across batches, got %d", len(rows))
	}
	for i := 0; i < 5; i++ {
		m := rows[i].(map[string]any)
		wantDP := fmt.Sprintf("winccoa/types/Scalar/BatchDP_%03d", 149-i)
		if m["topic"] != wantDP {
			t.Fatalf("row %d topic = %v, want %s", i, m["topic"], wantDP)
		}
	}
}

func TestWinCCOaArchiveHistoryNoArchiveConfig(t *testing.T) {
	env, url := startNativeGraphQL(t, 27720, 28720, nil)
	if err := env.sim.SetArchived("System1:Pump1.speed", false); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := env.sim.SetTimed("System1:Pump101.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 55.5}, now.Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}

	resNoArc := gqlQuery(t, url, `{
		archivedMessages(topicFilter: "winccoa/tags/Pump1/speed", archiveGroup: "WinCCOA") { topic }
	}`, nil)
	if got := len(resNoArc["archivedMessages"].([]any)); got != 0 {
		t.Fatalf("expected 0 rows for unarchived DPE, got %d", got)
	}

	resWild := gqlQuery(t, url, `{
		archivedMessages(topicFilter: "winccoa/tags/+/speed", archiveGroup: "WinCCOA") { topic }
	}`, nil)
	rows := resWild["archivedMessages"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["topic"] != "winccoa/tags/Pump101/speed" {
		t.Fatalf("expected 1 row from archived Pump101.speed, got %v", rows)
	}
}

func TestWinCCOaArchiveAggregatedMatchesSQLite(t *testing.T) {
	env, url := startNativeGraphQL(t, 27721, 28721, nil)
	ctx := context.Background()

	// Create a SQLite archive group covering sql/#.
	gqlQuery(t, url, `mutation {
		archiveGroup {
			create(input: {
				name: "SqlCompare"
				topicFilter: ["sql/#"]
				lastValType: SQLITE
				archiveType: SQLITE
				bulkTimeoutMs: 50
			}) { success }
		}
	}`, nil)
	sqlGroup := env.srv.Archives().Get("SqlCompare")
	if sqlGroup == nil || sqlGroup.Archive() == nil {
		t.Fatal("SqlCompare archive not deployed")
	}

	t0 := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	samples := []struct {
		offset time.Duration
		val    float64
	}{
		{1 * time.Minute, 10},
		{2 * time.Minute, 20},
		{6 * time.Minute, 30},
		{8 * time.Minute, 50},
	}

	var sqlMsgs []stores.BrokerMessage
	for _, s := range samples {
		ts := t0.Add(s.offset)
		if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: s.val}, ts); err != nil {
			t.Fatal(err)
		}
		payload := fmt.Sprintf(`{"value":%v,"time":%q,"type":"float"}`, s.val, ts.Format("2006-01-02T15:04:05.000Z"))
		sqlMsgs = append(sqlMsgs, stores.BrokerMessage{
			TopicName: "sql/Pump1/speed",
			Payload:   []byte(payload),
			Time:      ts,
		})
	}
	if err := sqlGroup.Archive().AddHistory(ctx, sqlMsgs); err != nil {
		t.Fatalf("sql AddHistory: %v", err)
	}

	startStr := t0.Format(time.RFC3339)
	endStr := t0.Add(15 * time.Minute).Format(time.RFC3339)
	queryFmt := `{
		aggregatedMessages(
			topics: [%q]
			interval: FIVE_MINUTES
			startTime: %q
			endTime: %q
			functions: [AVG, MIN, MAX, COUNT]
			fields: ["value"]
			archiveGroup: %q
		) {
			columns
			rows
			rowCount
		}
	}`
	oaRes := gqlQuery(t, url, fmt.Sprintf(queryFmt, "winccoa/tags/Pump1/speed", startStr, endStr, "WinCCOA"), nil)["aggregatedMessages"].(map[string]any)
	sqlRes := gqlQuery(t, url, fmt.Sprintf(queryFmt, "sql/Pump1/speed", startStr, endStr, "SqlCompare"), nil)["aggregatedMessages"].(map[string]any)

	oaRowsJS, _ := json.Marshal(oaRes["rows"])
	sqlRowsJS, _ := json.Marshal(sqlRes["rows"])
	if string(oaRowsJS) != string(sqlRowsJS) {
		t.Fatalf("rows mismatch:\nWINCCOA: %s\nSQLITE:  %s", oaRowsJS, sqlRowsJS)
	}
}

func TestWinCCOaArchiveAggregatedPaging(t *testing.T) {
	defer oastore.SetAggPageSamplesForTest(3)()

	env, url := startNativeGraphQL(t, 27722, 28722, nil)
	t0 := time.Date(2026, 10, 17, 11, 0, 0, 0, time.UTC)

	// 8 samples in the same 5-minute bucket, with two sharing a timestamp at the page boundary (index 2 and 3).
	times := []time.Duration{
		10 * time.Second,
		20 * time.Second,
		30 * time.Second,
		30 * time.Second, // duplicate ms at page boundary 3
		40 * time.Second,
		50 * time.Second,
		60 * time.Second,
		70 * time.Second,
	}
	for i, d := range times {
		if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: float64(i + 1)}, t0.Add(d)); err != nil {
			t.Fatal(err)
		}
	}

	before := env.sim.Calls(oahost.OpDpGetPeriod)
	res := gqlQuery(t, url, fmt.Sprintf(`{
		aggregatedMessages(
			topics: ["winccoa/tags/Pump1/speed"]
			interval: FIVE_MINUTES
			startTime: %q
			endTime: %q
			functions: [COUNT, AVG]
			archiveGroup: "WinCCOA"
		) {
			rows
		}
	}`, t0.Format(time.RFC3339), t0.Add(5*time.Minute).Format(time.RFC3339)), nil)["aggregatedMessages"].(map[string]any)
	after := env.sim.Calls(oahost.OpDpGetPeriod)
	if after-before < 3 {
		t.Fatalf("expected >= 3 paged OpDpGetPeriod calls, got %d", after-before)
	}
	rows := res["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected 1 bucket row, got %v", rows)
	}
	row := rows[0].([]any)
	// COUNT should be 8, AVG of 1..8 should be 4.5.
	if row[1] != float64(8) || row[2] != 4.5 {
		t.Fatalf("unexpected [bucket, COUNT, AVG] = %v, want [..., 8, 4.5]", row)
	}
}

func TestWinCCOaArchiveAggregatedTooLarge(t *testing.T) {
	env, url := startNativeGraphQL(t, 27723, 28723, nil)
	before := env.sim.Calls(oahost.OpDpGetPeriod)

	// 3 years at ONE_MINUTE (~1.57M buckets) exceeds 1,000,000 cells.
	raw := gqlRequest(t, url, `{
		aggregatedMessages(
			topics: ["winccoa/tags/Pump1/speed"]
			interval: ONE_MINUTE
			startTime: "2023-01-01T00:00:00Z"
			endTime: "2026-01-01T00:00:00Z"
			functions: [AVG]
			archiveGroup: "WinCCOA"
		) { rowCount }
	}`, nil, "")
	if !graphqlErrorContains(raw, "limit 1000000") {
		t.Fatalf("expected cell-cap error, got %v", raw)
	}
	if env.sim.Calls(oahost.OpDpGetPeriod) != before {
		t.Fatal("OpDpGetPeriod should not be called when cell cap is exceeded")
	}
}

func TestWinCCOaArchiveAggregatedMixedGroups(t *testing.T) {
	env, url := startNativeGraphQL(t, 27724, 28724, nil)
	ctx := context.Background()

	// Configure Default with a SQLite archive covering sensors/#.
	gqlQuery(t, url, `mutation {
		archiveGroup {
			update(input: {
				name: "Default"
				topicFilter: ["sensors/#"]
				archiveType: SQLITE
				archiveReadOnly: false
				bulkTimeoutMs: 50
			}) { success }
		}
	}`, nil)
	defGroup := env.srv.Archives().Get("Default")
	if defGroup == nil || defGroup.Archive() == nil {
		t.Fatal("Default SQLite archive not deployed")
	}

	t0 := time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)
	if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 100}, t0.Add(1*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := defGroup.Archive().AddHistory(ctx, []stores.BrokerMessage{{
		TopicName: "sensors/temp",
		Payload:   []byte(`{"value":25.5}`),
		Time:      t0.Add(1 * time.Minute),
	}}); err != nil {
		t.Fatalf("Default AddHistory: %v", err)
	}

	res := gqlQuery(t, url, fmt.Sprintf(`{
		aggregatedMessages(
			topics: ["winccoa/tags/Pump1/speed", "sensors/temp"]
			interval: FIVE_MINUTES
			startTime: %q
			endTime: %q
			functions: [AVG]
			fields: ["value"]
		) {
			columns
			rows
		}
	}`, t0.Format(time.RFC3339), t0.Add(5*time.Minute).Format(time.RFC3339)), nil)["aggregatedMessages"].(map[string]any)

	colsJS, _ := json.Marshal(res["columns"])
	wantCols := `["timestamp","winccoa/tags/Pump1/speed.value_avg","sensors/temp.value_avg"]`
	if string(colsJS) != wantCols {
		t.Fatalf("columns = %s, want %s", colsJS, wantCols)
	}
	rows := res["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("rows = %v", rows)
	}
	row := rows[0].([]any)
	if row[1] != float64(100) || row[2] != 25.5 {
		t.Fatalf("merged row = %v, want [..., 100, 25.5]", row)
	}
}

func TestWinCCOaArchiveFallbackGraphQL(t *testing.T) {
	env, url := startNativeGraphQL(t, 27725, 28725, nil)
	ctx := context.Background()

	ts := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Minute)
	if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 77}, ts); err != nil {
		t.Fatal(err)
	}

	// Omitted archiveGroup and "Default" both resolve native last-value and history to WinCCOA when Default has ArchiveType=NONE.
	for _, groupArg := range []string{"", `, archiveGroup: "Default"`, `, archiveGroup: "WinCCOA"`} {
		data := gqlQuery(t, url, fmt.Sprintf(`{
			cv: currentValue(topic: "winccoa/tags/Pump1/speed"%s) { payload }
			cvs: currentValues(topicFilter: "winccoa/tags/Pump1/speed"%s) { payload }
			hist: archivedMessages(topicFilter: "winccoa/tags/Pump1/speed"%s) { payload }
		}`, groupArg, groupArg, groupArg), nil)
		if data["cv"] == nil || len(data["cvs"].([]any)) != 1 || len(data["hist"].([]any)) != 1 {
			t.Fatalf("groupArg %q failed fallback: %v", groupArg, data)
		}
	}

	// Now give Default a SQLite archive covering winccoa/#: history on Default stays on Default's SQLite archive, while currentValue still routes to WinCCOA.
	gqlQuery(t, url, `mutation {
		archiveGroup {
			update(input: {
				name: "Default"
				topicFilter: ["winccoa/#"]
				archiveType: SQLITE
				archiveReadOnly: false
				bulkTimeoutMs: 50
			}) { success }
		}
	}`, nil)
	defArc := env.srv.Archives().Get("Default").Archive()
	if err := defArc.AddHistory(ctx, []stores.BrokerMessage{{
		TopicName: "winccoa/tags/Pump1/speed",
		Payload:   []byte(`{"value":1234,"from":"sqlite"}`),
		Time:      ts,
	}}); err != nil {
		t.Fatal(err)
	}

	data := gqlQuery(t, url, `{
		cv: currentValue(topic: "winccoa/tags/Pump1/speed") { payload }
		histDef: archivedMessages(topicFilter: "winccoa/tags/Pump1/speed") { payload }
		histOA: archivedMessages(topicFilter: "winccoa/tags/Pump1/speed", archiveGroup: "WinCCOA") { payload }
	}`, nil)
	if !strings.Contains(fmt.Sprint(data["cv"].(map[string]any)["payload"]), "77") {
		t.Fatalf("expected currentValue from WinCCOA (77), got %v", data["cv"])
	}
	histDef := data["histDef"].([]any)
	if len(histDef) != 1 || !strings.Contains(fmt.Sprint(histDef[0].(map[string]any)["payload"]), "sqlite") {
		t.Fatalf("expected Default history from SQLite when Default covers winccoa/#, got %v", histDef)
	}
	histOA := data["histOA"].([]any)
	if len(histOA) != 1 || !strings.Contains(fmt.Sprint(histOA[0].(map[string]any)["payload"]), "77") {
		t.Fatalf("expected explicit WinCCOA history from OA, got %v", histOA)
	}
}

func TestWinCCOaArchiveFallbackREST(t *testing.T) {
	env, _ := startNativeGraphQL(t, 27726, 28726, func(c *config.Config) {
		c.RestApi.Enabled = true
	})
	ts := time.Now().UTC().Truncate(time.Second).Add(-5 * time.Minute)
	if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 88.5}, ts); err != nil {
		t.Fatal(err)
	}

	baseURL := "http://localhost:28726/api/v1/topics/winccoa/tags/Pump1/speed"
	for _, q := range []string{"", "?group=Default", "?group=WinCCOA"} {
		resp, err := http.Get(baseURL + q)
		if err != nil {
			t.Fatalf("GET %s: %v", q, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "88.5") {
			t.Fatalf("GET %s: status=%d body=%s", q, resp.StatusCode, body)
		}
	}

	// History via REST with start/end and omitted group.
	histURL := fmt.Sprintf("%s?start=%s&end=%s", baseURL, ts.Add(-time.Minute).Format(time.RFC3339), ts.Add(time.Minute).Format(time.RFC3339))
	resp, err := http.Get(histURL)
	if err != nil {
		t.Fatalf("GET history: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "88.5") {
		t.Fatalf("GET history: status=%d body=%s", resp.StatusCode, body)
	}

	// Unknown group returns 404.
	resp404, err := http.Get(baseURL + "?group=NoSuchGroup")
	if err != nil {
		t.Fatal(err)
	}
	resp404.Body.Close()
	if resp404.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown group, got %d", resp404.StatusCode)
	}
}

func TestWinCCOaArchiveFallbackMCP(t *testing.T) {
	env, _ := startNativeGraphQL(t, 27727, 28727, func(c *config.Config) {
		c.MCP.Enabled = true
		c.Features.Mcp = true
	})
	mcpURL := "http://localhost:28727/mcp"
	ts := time.Date(2026, 10, 17, 15, 1, 0, 0, time.UTC)
	if err := env.sim.SetTimed("System1:Pump1.speed:_original.._value", oahost.Value{Kind: oahost.KindFloat, Float: 64.25}, ts); err != nil {
		t.Fatal(err)
	}

	callTool := func(name string, args string) string {
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":%q,"arguments":%s}}`, name, args)
		status, resp := mcpRequest(t, mcpURL, body, "")
		if status != http.StatusOK {
			t.Fatalf("MCP %s status=%d: %s", name, status, resp)
		}
		return resp
	}

	gotVal := callTool("get-topic-value", `{"topics":["winccoa/tags/Pump1/speed"]}`)
	if !strings.Contains(gotVal, "64.25") {
		t.Fatalf("MCP get-topic-value: %s", gotVal)
	}

	gotHist := callTool("query-message-archive", fmt.Sprintf(`{"topic":"winccoa/tags/Pump1/speed","startTime":%q,"endTime":%q}`,
		ts.Add(-time.Minute).Format(time.RFC3339), ts.Add(time.Minute).Format(time.RFC3339)))
	if !strings.Contains(gotHist, "64.25") {
		t.Fatalf("MCP query-message-archive: %s", gotHist)
	}

	gotAgg := callTool("query-message-archive-aggregated", fmt.Sprintf(`{"topics":["winccoa/tags/Pump1/speed"],"interval":"FIVE_MINUTES","startTime":%q,"endTime":%q,"functions":["AVG"]}`,
		ts.Add(-time.Minute).Format(time.RFC3339), ts.Add(5*time.Minute).Format(time.RFC3339)))
	if !strings.Contains(gotAgg, "64.25") {
		t.Fatalf("MCP query-message-archive-aggregated: %s", gotAgg)
	}
}

func TestWinCCOaArchiveBrowseLevels(t *testing.T) {
	_, url := startNativeGraphQL(t, 27728, 28728, nil)

	browse := func(pattern string, group string) map[string]bool {
		q := fmt.Sprintf(`{ browseTopics(topic: %q, archiveGroup: %q) { name isLeaf } }`, pattern, group)
		if group == "" {
			q = fmt.Sprintf(`{ browseTopics(topic: %q) { name isLeaf } }`, pattern)
		}
		res := gqlQuery(t, url, q, nil)["browseTopics"].([]any)
		out := make(map[string]bool, len(res))
		for _, item := range res {
			m := item.(map[string]any)
			out[m["name"].(string)] = m["isLeaf"].(bool)
		}
		return out
	}

	if got := browse("+", "WinCCOA"); len(got) != 1 || got["winccoa"] != false {
		t.Fatalf("browse(+): %v", got)
	}
	if got := browse("winccoa/+", "WinCCOA"); len(got) != 3 || got["winccoa/tags"] || got["winccoa/types"] || got["winccoa/systems"] {
		t.Fatalf("browse(winccoa/+): %v", got)
	}
	if got := browse("winccoa/systems/+", "WinCCOA"); len(got) != 2 || got["winccoa/systems/System1"] || got["winccoa/systems/SubstationA"] {
		t.Fatalf("browse(winccoa/systems/+): %v", got)
	}
	tags := browse("winccoa/tags/+", "")
	if len(tags) != 3 || tags["winccoa/tags/Pump1"] != false || tags["winccoa/tags/Pump101"] != false || tags["winccoa/tags/ScalarTag"] != true {
		t.Fatalf("browse(winccoa/tags/+): %v", tags)
	}
	pump1 := browse("winccoa/tags/Pump1/+", "")
	if pump1["winccoa/tags/Pump1/speed"] != true || pump1["winccoa/tags/Pump1/nested"] != false {
		t.Fatalf("browse(winccoa/tags/Pump1/+): %v", pump1)
	}
	if _, hasSet := pump1["winccoa/tags/Pump1/%73et"]; !hasSet {
		t.Fatalf("expected encoded element '%%73et' to be present in Pump1/+: %v", pump1)
	}
	nested := browse("winccoa/tags/Pump1/nested/+", "")
	if len(nested) != 1 || nested["winccoa/tags/Pump1/nested/a"] != true {
		t.Fatalf("browse(winccoa/tags/Pump1/nested/+): %v", nested)
	}
	types := browse("winccoa/types/+", "")
	if types["winccoa/types/AnalogDrive"] != false || types["winccoa/types/Scalar"] != false {
		t.Fatalf("browse(winccoa/types/+): %v", types)
	}
	analogDPs := browse("winccoa/types/AnalogDrive/+", "")
	if len(analogDPs) != 2 || analogDPs["winccoa/types/AnalogDrive/Pump1"] != false || analogDPs["winccoa/types/AnalogDrive/Pump101"] != false {
		t.Fatalf("browse(winccoa/types/AnalogDrive/+): %v", analogDPs)
	}
}

func TestWinCCOaArchiveSearchTopicsBounded(t *testing.T) {
	sim, client := newSim(0)
	defer sim.Close()
	for i := 0; i < 200; i++ {
		if err := sim.CreateDP("System1", fmt.Sprintf("SearchDP_%03d", i), "Scalar"); err != nil {
			t.Fatal(err)
		}
	}
	env := startNative(t, 27729, filepath.Join(t.TempDir(), "search.db"), sim, client, func(c *config.Config) {
		c.GraphQL.Enabled = true
		c.GraphQL.Port = 28729
		c.GraphQL.TLSPort = 29729
		c.WinCCOaNative.QueryMaxRows = 100
	}, broker.Options{})
	defer env.srv.Close()
	url := "http://localhost:28729/graphql"
	waitForHealth(t, 28729)

	res := gqlQuery(t, url, `{ searchTopics(pattern: "SearchDP", limit: 500, archiveGroup: "WinCCOA") }`, nil)
	list := res["searchTopics"].([]any)
	if len(list) == 0 || len(list) > 100 {
		t.Fatalf("expected searchTopics bounded by QueryMaxRows=100, got %d", len(list))
	}
}

func TestWinCCOaArchiveInterestFilters(t *testing.T) {
	env, _ := startNativeGraphQL(t, 27730, 28730, nil)
	filters := env.srv.Archives().InterestFilters()
	for _, f := range filters {
		if f == "winccoa/#" {
			t.Fatalf("InterestFilters() should skip read-only WinCCOA group, got %v", filters)
		}
	}
}

func waitForHealth(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port)); err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

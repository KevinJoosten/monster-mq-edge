# Plan: WinCC OA Archiving (`_archive` Config & Archive Group Selection) in UNS Modeler

- **Status:** Completed
- **Issue:** [vogler75/monster-mq-edge#41](https://github.com/vogler75/monster-mq-edge/issues/41)
- **Date:** 2026-10-17
- **Target:** `edge/internal/unsmodeler/`, `edge/internal/winccoabrowse/`, `edge/winccoa/manager/`, `dashboard/src/`
- **Related Documents:**
  - [`edge/winccoa/dev/plans/plan-uns-modeler-design.md`](plan-uns-modeler-design.md)
  - [`edge/winccoa/doc/uns-modeler.md`](../../doc/uns-modeler.md)
  - [`edge/internal/unsmodeler/create_b.go`](../../../internal/unsmodeler/create_b.go)
  - [`edge/internal/unsmodeler/validate_b.go`](../../../internal/unsmodeler/validate_b.go)

---

## 1. Goal & Motivation

When the WinCC OA native UNS Modeler onboards MQTT topics into WinCC OA datapoints (**Direction B: `MQTT_TO_WINCCOA`**), `CreateInWinCCOa` creates datapoint types (DPTs), creates datapoints (DPs), and configures element metadata (`unit`, `description`, `format`, `_pv_range`, `_alert_hdl`), followed by starting the live value flow (`runtime_b.go`).

Currently, created datapoint elements have no WinCC OA `_archive` configuration (`DPCONFIG_NONE`), meaning values written by the UNS Modeler flow are only held as current online values (`_online.._value`) in the Event/Data Manager and are **not recorded** into WinCC OA's historical archive (Next Generation Archiver / NGA, ValueArchive / HDB, or RDB).

This plan adds first-class support for enabling WinCC OA archiving (`_archive` config) in the UNS Modeler:
1. **Selectable WinCC OA Archive Group:** Discovers available value archive groups directly from WinCC OA (`_NGA_Group`, `_ValueArchive`, `_RDBArchiveGroups`) and lets the user select the target archive group (e.g. `EVENT`, `DEFAULT`, `_ValueArchive_0`).
2. **Device-Level Default + Per-Element Override:**
   - **Device Configuration (Direction B):** Toggle *"Archive values in WinCC OA"* (`archiveEnabled`) and select a default *"WinCC OA Archive Group"* (`archiveGroup`) so all created datapoint elements are archived automatically without manual per-element configuration.
   - **Type / Datapoint Element Metadata (`archive`):** Optional `archive: { "enabled": true|false, "group": "..." }` metadata in the Direction B mapping document (`types[].elements[]` and `datapoints[].elements[]`) to override the device default per type or per datapoint element (e.g. excluding diagnostic elements from archiving or routing high-frequency vs. slow measurements to different archive groups).
3. **WinCC OA 3.21 `_archive` Config Provisioning:** Writes the exact `_archive` config attributes required by WinCC OA 3.21 (`DPCONFIG_DB_ARCHIVEINFO = 45`, `DPATTR_ARCH_PROC_VALARCH = 15`, `_archive.1._class` as `DpIdentifierVar`, `_archive.._archive = true`) during `CreateInWinCCOa`.

---

## 2. WinCC OA 3.21 Archiving Mechanics (SDK & CTRL Reference)

Verified against the local WinCC OA 3.21 installation (`C:/Program Files/Siemens/WinCC_OA/3.21`) and project `C:/WinCC_OA_Proj/Test321` (`useNGA = 1`, `useSQLite = 1`):

### 2.1 `_archive` Config Constants & Attributes

From `api/include/Configs/DpConfig.hxx`, `api/include/Configs/ConfigTypes.hxx`, `scripts/libs/ac.ctl`, `scripts/libs/ngaUtils.ctl`, `scripts/libs/classes/nga/NgaParaConfigProvider.ctl`, and `scripts/libs/para.ctl`:

| Constant / Attribute | Value / Type | Reference | Description |
|---|---|---|---|
| `DPCONFIG_NONE` | `0` (`INTEGER_VAR`) | `DpConfig.hxx:31` | No archive config on the DPE |
| `DPCONFIG_DB_ARCHIVEINFO` | `45` (`INTEGER_VAR`) | `DpConfig.hxx:91` | Standard `_archive` config type (`_archive.._type`) |
| `DPATTR_ARCH_PROC_NONE` | `0` (`INTEGER_VAR`) | `ConfigTypes.hxx:132` | `ArchProcNothing` |
| `DPATTR_ARCH_PROC_SIMPLESM` | `3` (`INTEGER_VAR`) | `ConfigTypes.hxx:135` | `ArchProcSimpleSmooth` (when smoothing is active) |
| `DPATTR_ARCH_PROC_VALARCH` | `15` (`INTEGER_VAR`) | `ConfigTypes.hxx:145` | `ArchProcValueArchive` (value archiving without smoothing) |
| `<dpe>:_archive.._type` | `int = 45` | `ngaUtils.ctl:2158` | Creates the `_archive` config on `<dpe>` |
| `<dpe>:_archive.1._type` | `int = 15` | `ngaUtils.ctl:2159`, `ac.ctl:1130` | Creates archive procedure detail `1` (`DPATTR_ARCH_PROC_VALARCH`) |
| `<dpe>:_archive.1._proc` | `int = 15` | `NgaParaConfigProvider.ctl:358` | Sets procedure `1` to `DPATTR_ARCH_PROC_VALARCH` (`15`, no smoothing) |
| `<dpe>:_archive.1._class` | `DpIdentifierVar` (`DPELEMENT_DPID`) | `ngaUtils.ctl:2160`, `para.ctl:8328` | Target archive group datapoint identifier, e.g. `System1:_NGA_G_EVENT.` or `System1:_ValueArchive_0.` |
| `<dpe>:_archive.._archive` | `bool = true` | `ngaUtils.ctl:2158`, `para.ctl:8326` | Activates archiving for the DPE |

**Two-Step `dpSet` Ordering (same pattern as `_alert_hdl` in `create_b.go`):**
Just as `_alert_hdl.._type` and `_alert_hdl.<n>._type` must be set in a first `dpSet` before setting `_alert_hdl.<n>._class` and `_alert_hdl.._active`, WinCC OA requires creating the `_archive` config and detail `1` before setting detail `1`'s `_proc`/`_class` and activating `_archive.._archive`:
1. **Step 1 (`dpSet`):**
   - `<dpe>:_archive.._type` = `45` (`DPCONFIG_DB_ARCHIVEINFO`, `KindInt`)
   - `<dpe>:_archive.1._type` = `15` (`DPATTR_ARCH_PROC_VALARCH`, `KindInt`)
2. **Step 2 (`dpSet`):**
   - `<dpe>:_archive.1._proc` = `15` (`DPATTR_ARCH_PROC_VALARCH`, `KindInt`)
   - `<dpe>:_archive.1._class` = `"<localSystem>:<archiveClassDp>."` (`KindString`, converted to `DpIdentifierVar` by `WCCOAmmq`)
   - `<dpe>:_archive.._archive` = `true` (`KindBool`)

### 2.2 How WinCC OA Discovers Archive Groups

WinCC OA supports three archiving backends depending on project configuration (`NGA`, `ValueArchive` / HDB, and `RDB`). From `NgaParaConfigProvider.ctl:1094-1104` and `panels/para/archive.pnl:85-150`:

1. **NGA (`_NGA_Group` datapoints, standard in WinCC OA 3.21 / `Test321`):**
   - Datapoint type: `_NGA_Group`, naming convention: `_NGA_G_<GROUP_NAME>` (e.g. `_NGA_G_EVENT`, `_NGA_G_DEFAULT`, `_NGA_G_RDB_99_1_EVENT`).
   - Redundant replica datapoints ending in `_2` (`*_2`) are excluded.
   - Alert archive groups where `_NGA_G_<name>.isAlert == true` (e.g. `_NGA_G_ALERT`, `_NGA_G_RDB_99_2_ALERT`) are **excluded** because `_archive` on a DPE only accepts value archive groups.
   - Display/short name: `_NGA_G_<name>.displayName` (or DP name stripped of `_NGA_G_`, e.g. `EVENT`, `DEFAULT`).
   - Value written to `_archive.1._class`: `<System>:_NGA_G_<name>.` (e.g. `System1:_NGA_G_EVENT.`).

2. **HDB / ValueArchive (`_ValueArchive` datapoints):**
   - Datapoint type: `_ValueArchive`, naming convention: `_ValueArchive_<N>` (e.g. `_ValueArchive_0`).
   - Redundant replica datapoints (`*_2`) and deleted archives (`_ValueArchive_<N>.state == 3`, `ARCHIVE_STATUS_DELETED`) are excluded.
   - Display name: `_ValueArchive_<N>.general.arName` (e.g. `ValueArchive_0`).
   - Value written to `_archive.1._class`: `<System>:_ValueArchive_<N>.` (e.g. `System1:_ValueArchive_0.`).

3. **RDB (`_RDBArchiveGroups` datapoints):**
   - Datapoint type: `_RDBArchiveGroups`.
   - Redundant replica datapoints (`*_2`), alert groups (`.isAlert == true`), and compressed-step templates (`.templateName == "CS"`) are excluded.
   - Display name: `.groupName`.
   - Value written to `_archive.1._class`: `<System>:<dpName>.`.

### 2.3 Two Important Codebase Findings to Address

1. **`MmqManager::opDpSet` (`edge/winccoa/manager/MmqManager.cxx:1023-1034`) decoding of `_archive.1._class` on `string` (`DPELEMENT_TEXT`) DPEs:**
   - Currently, `opDpSet` calls `decodeValue(*values[i], et, err)` with the DPE's own value type `et` first, and only checks `attributeElementType(id)` (`if (!v)`) when `decodeValue` fails:
     ```cpp
     DpElementType et = DPELEMENT_NOELEMENT;
     getElementType(id, et);
     std::string err;
     Variable *v = decodeValue(*values[i], et, err);
     if (!v)
     {
       DpElementType at = attributeElementType(id);
       std::string aerr;
       if (at != DPELEMENT_NOELEMENT && at != et)
         v = decodeValue(*values[i], at, aerr);
     }
     ```
   - Why this worked for `_alert_hdl.._class` previously: `_alert_hdl` in UNS Modeler is restricted to `bool`, `int`, `uint`, and `float` elements (`et != DPELEMENT_TEXT`), so passing a `KindString` for `._class` always failed the first `decodeValue` and fell through to `at = DPELEMENT_DPID`.
   - **What happens when archiving a `string` (`DPELEMENT_TEXT`) DPE:** `et` is `DPELEMENT_TEXT`, so `decodeValue(*values[i], et, err)` with a `KindString` value (`"System1:_NGA_G_EVENT."`) **succeeds** on the first call and produces a `TextVar` instead of a `DpIdentifierVar` (`DPELEMENT_DPID`), causing WinCC OA Data Manager to reject `_archive.1._class`!
   - **Fix in `MmqManager::opDpSet`:** Query `DpElementType at = attributeElementType(id)` first; when `at != DPELEMENT_NOELEMENT` (which holds for all typed config attributes such as `INTEGER_VAR`, `BIT_VAR`, `DPIDENTIFIER_VAR`, `LANGTEXT_VAR`, whereas `_online.._value` and `_original.._value` have `ANYTYPE_VAR` $\rightarrow$ `DPELEMENT_NOELEMENT`), decode with `at` first and fall back to `et`!

2. **`winccoabrowse.Service.MatchDatapoints` filters out leading-underscore DPs (`winccoanative.Protected`):**
   - `MatchDatapoints` in `edge/internal/winccoabrowse/browse.go:561` skips any datapoint where `winccoanative.Protected(dp)` is true (`strings.HasPrefix(dp, "_")`).
   - While `_AlertClass` datapoints are named `alert`, `warning`, etc. (no leading underscore), **all WinCC OA archive group datapoints start with `_`** (`_NGA_G_*`, `_ValueArchive_*`, `_RDB*`).
   - Therefore, `winccoabrowse.Service` needs a dedicated `ArchiveGroups(ctx, system)` method that queries `_NGA_Group`, `_ValueArchive`, and `_RDBArchiveGroups` via `s.api.DpNames` and `s.api.DpGet` directly without filtering out `_NGA_G_*` / `_ValueArchive_*`.

---

## 3. Proposed GraphQL Interface Changes (Requires Human Approval)

> [!IMPORTANT]
> Per `edge/AGENTS.md`, **no GraphQL schema files (`internal/graphql/schema/*.graphqls`) or resolvers will be modified without explicit human approval of the changes below.**

### 3.1 `edge/internal/graphql/schema/winccoa-browse.graphqls`

Expose the discovered WinCC OA archive groups on `WinCCOaQuery` so the dashboard can populate the archive group dropdown in UNS Modeler:

```graphql
"A WinCC OA value archive group (NGA _NGA_Group, HDB _ValueArchive, or RDB _RDBArchiveGroups)."
type WinCCOaArchiveGroup {
    "Short group name used in mapping documents and settings, e.g. EVENT, DEFAULT, ValueArchive_0."
    name: String!
    "Underlying WinCC OA datapoint name, e.g. _NGA_G_EVENT or _ValueArchive_0."
    dpName: String!
    "Human-readable display name from WinCC OA."
    displayName: String!
    "Archive backend kind: NGA, HDB, or RDB."
    kind: String!
}

extend type WinCCOaQuery {
    "Value archive groups available in the WinCC OA system (NGA, HDB/ValueArchive, RDB)."
    archiveGroups(system: String): [WinCCOaArchiveGroup!]!
}
```

### 3.2 `edge/internal/graphql/schema/unsmodeler.graphqls`

Add `oaArchiveEnabled` and `oaArchiveGroup` to `UnsModelerConfig` and `UnsModelerConfigInput` for Direction B device-level defaults (named `oaArchiveEnabled` / `oaArchiveGroup` to distinguish them clearly from `UnsModelerAiRunInput.archiveGroup`, which is the MonsterMQ topic-source archive group):

```graphql
type UnsModelerConfig {
    # ... existing fields ...
    "Direction B: mandatory prefix of created types and datapoints."
    oaPrefix: String
    "Direction B: write values after create."
    flowEnabled: Boolean!
    "Direction B: enable WinCC OA _archive config on created datapoint elements by default."
    oaArchiveEnabled: Boolean!
    "Direction B: default WinCC OA archive group name (e.g. EVENT, DEFAULT, _NGA_G_EVENT, _ValueArchive_0)."
    oaArchiveGroup: String
}

input UnsModelerConfigInput {
    # ... existing fields ...
    oaPrefix: String
    flowEnabled: Boolean
    oaArchiveEnabled: Boolean
    oaArchiveGroup: String
}
```

---

## 4. Mapping Document Schema & Settings Design (`edge/internal/unsmodeler`)

### 4.1 Device Configuration (`settings.go`)

Extend `Settings` in [`edge/internal/unsmodeler/settings.go`](../../../internal/unsmodeler/settings.go):

```go
type Settings struct {
    // ... existing fields ...

    // Direction B
    OaPrefix         string `json:"oaPrefix,omitempty"`
    FlowEnabled      *bool  `json:"flowEnabled,omitempty"`
    OaArchiveEnabled bool   `json:"oaArchiveEnabled,omitempty"`
    OaArchiveGroup   string `json:"oaArchiveGroup,omitempty"`
}
```

**Settings Validation (`ValidateSettings` in `settings.go`):**
- When `s.Direction == DirectionMqttToWinccoa`:
  - If `s.OaArchiveEnabled && strings.TrimSpace(s.OaArchiveGroup) == ""` $\rightarrow$ `SeverityError` at `/oaArchiveGroup`: `"is required when WinCC OA archiving is enabled"`.
  - If `s.OaArchiveGroup != "" && !oaNameRe.MatchString(s.OaArchiveGroup)` $\rightarrow$ `SeverityError` at `/oaArchiveGroup`: `"must be a valid WinCC OA archive group name"`.

### 4.2 Mapping JSON Schema (`schema/mapping.schema.json`) & Document Model (`document.go`)

1. **`$defs/Archive` in [`edge/internal/unsmodeler/schema/mapping.schema.json`](../../../internal/unsmodeler/schema/mapping.schema.json):**
   ```json
   "Archive": {
     "type": "object",
     "additionalProperties": false,
     "required": ["enabled"],
     "properties": {
       "enabled": { "type": "boolean" },
       "group": { "$ref": "#/$defs/OaName" }
     }
   }
   ```
2. **Add `"archive": { "$ref": "#/$defs/Archive" }` to `$defs/TypeElement` and `$defs/DatapointElement`:**
   - All Direction B value element types (`bool`, `int`, `uint`, `float`, `string`, `time`, `bit32`, `blob`) support `_archive` in WinCC OA, so no element-type restriction is needed in `$defs/TypeElement`.
3. **Go Structs in [`edge/internal/unsmodeler/document.go`](../../../internal/unsmodeler/document.go):**
   ```go
   // Archive configures the WinCC OA _archive config of a value element.
   type Archive struct {
       Enabled bool   `json:"enabled"`
       Group   string `json:"group,omitempty"`
   }
   ```
   Add `Archive *Archive `json:"archive,omitempty"`` to `TypeElement` and `DatapointElement`.

### 4.3 Effective Archive Resolution & Inheritance (`create_b.go`)

Just like `unit`, `description`, `format`, `pvRange`, and `alert`, `archive` follows a 3-tier inheritance chain:
$$\text{Device Default } (\texttt{Settings}) \longrightarrow \text{Type Element } (\texttt{types[].elements[]}) \longrightarrow \text{Datapoint Element } (\texttt{datapoints[].elements[]})$$

```go
func effectiveArchive(s *Settings, te *TypeElement, de *DatapointElement) *Archive {
    var enabled bool
    var group string
    if s != nil {
        enabled = s.OaArchiveEnabled
        group = s.OaArchiveGroup
    }
    if te != nil && te.Archive != nil {
        enabled = te.Archive.Enabled
        if te.Archive.Group != "" {
            group = te.Archive.Group
        }
    }
    if de != nil && de.Archive != nil {
        enabled = de.Archive.Enabled
        if de.Archive.Group != "" {
            group = de.Archive.Group
        }
    }
    if !enabled && group == "" {
        return nil
    }
    return &Archive{Enabled: enabled, Group: group}
}
```
- If the device has `oaArchiveEnabled: true, oaArchiveGroup: "EVENT"`, every value element of every created datapoint inherits `{Enabled: true, Group: "EVENT"}` automatically.
- A type element or datapoint element can override the group (`{"enabled": true, "group": "DEFAULT"}`) or opt out (`{"enabled": false}`).
- If the device has `oaArchiveEnabled: false`, individual type or datapoint elements can still opt in (`{"enabled": true, "group": "EVENT"}`).

---

## 5. WinCC OA Archive Group Discovery & Validation (`winccoabrowse` & `validate_b.go`)

### 5.1 `winccoabrowse.ArchiveGroups` (`edge/internal/winccoabrowse/browse.go`)

Add `ArchiveGroup` struct and `ArchiveGroups(ctx context.Context, system string) ([]ArchiveGroup, error)` to `winccoabrowse.Service`:

```go
// ArchiveGroup is one WinCC OA value archive group (NGA, HDB/ValueArchive, or RDB).
type ArchiveGroup struct {
    Name        string `json:"name"`        // short name, e.g. "EVENT", "DEFAULT", "ValueArchive_0"
    DpName      string `json:"dpName"`      // DP name without system, e.g. "_NGA_G_EVENT", "_ValueArchive_0"
    DisplayName string `json:"displayName"` // human-readable name from WinCC OA
    Kind        string `json:"kind"`        // "NGA", "HDB", or "RDB"
}
```

**Discovery Algorithm:**
1. **NGA (`_NGA_Group`):**
   - Call `s.api.DpNames(ctx, sysName+":_NGA_G_*", "_NGA_Group", s.timeout)`.
   - Filter out redundant DPs (`strings.HasSuffix(dp, "_2")`).
   - Batch-read `<sys>:<dp>.isAlert` and `<sys>:<dp>.displayName` via `s.api.DpGet`.
   - Skip any group where `isAlert.Bool == true`.
   - Set `Name = strings.TrimPrefix(dp, "_NGA_G_")`, `DpName = dp`, `DisplayName = displayName.Str` (fallback to `Name`), `Kind = "NGA"`.
2. **HDB / ValueArchive (`_ValueArchive`):**
   - Call `s.api.DpNames(ctx, sysName+":_ValueArchive_*", "_ValueArchive", s.timeout)`.
   - Filter out redundant DPs (`strings.HasSuffix(dp, "_2")`).
   - Batch-read `<sys>:<dp>.state` and `<sys>:<dp>.general.arName` via `s.api.DpGet`.
   - Skip deleted archives (`state.Int == 3 || state.Uint == 3`).
   - Set `Name = arName.Str` (fallback to `dp`), `DpName = dp`, `DisplayName = Name`, `Kind = "HDB"`.
3. **RDB (`_RDBArchiveGroups`):**
   - Call `s.api.DpNames(ctx, sysName+":*", "_RDBArchiveGroups", s.timeout)`.
   - Filter out redundant DPs (`strings.HasSuffix(dp, "_2")`).
   - Batch-read `<sys>:<dp>.isAlert`, `<sys>:<dp>.templateName`, and `<sys>:<dp>.groupName` via `s.api.DpGet`.
   - Skip where `isAlert.Bool == true` or `templateName.Str == "CS"`.
   - Set `Name = groupName.Str` (fallback to `dp`), `DpName = dp`, `DisplayName = Name`, `Kind = "RDB"`.
4. Sort results by `Kind` (`NGA`, `HDB`, `RDB`) then `Name`.
5. **Matching / Resolution Helper (`ResolveArchiveGroup`):**
   - Matches a user-supplied group string `g` against `ArchiveGroup.Name`, `ArchiveGroup.DpName`, or `ArchiveGroup.DisplayName` (case-sensitive first, then case-insensitive fallback for convenience), returning `ArchiveGroup.DpName` (e.g. `"EVENT"` $\rightarrow$ `"_NGA_G_EVENT"`, `"_NGA_G_EVENT"` $\rightarrow$ `"_NGA_G_EVENT"`).

### 5.2 Stage 2 Validation (`validate_b.go`)

Extend `Browser` interface in [`edge/internal/unsmodeler/validate.go`](../../../internal/unsmodeler/validate.go) with:
```go
ArchiveGroups(ctx context.Context, system string) ([]winccoabrowse.ArchiveGroup, error)
```

In `validateB` ([`edge/internal/unsmodeler/validate_b.go`](../../../internal/unsmodeler/validate_b.go)):
1. **Check missing group when `enabled: true`:**
   - If `s.OaArchiveEnabled && s.OaArchiveGroup == ""`: report `SeverityError` at `/oaArchiveGroup`: `"archive group is required when device archiving is enabled"`.
   - For each type element `e` with `e.Archive != nil && e.Archive.Enabled && e.Archive.Group == "" && s.OaArchiveGroup == ""`: report `SeverityError` at `/types/<i>/elements/<j>/archive`: `"archive.group is required when archiving is enabled and no default archive group is set on the device"`.
   - For each datapoint element `e` with `e.Archive != nil && e.Archive.Enabled`: check that `e.Archive.Group != ""` or the type element has a group or `s.OaArchiveGroup != ""`; otherwise report `SeverityError` at `/datapoints/<i>/elements/<j>/archive`.
2. **Verify archive groups exist in WinCC OA (when `oa != nil`):**
   - Collect all referenced archive group names (`s.OaArchiveGroup` when `s.OaArchiveEnabled`, `te.Archive.Group`, `de.Archive.Group`) with their JSON pointers.
   - Call `oa.ArchiveGroups(ctx, "")` once and verify each referenced group resolves via `ResolveArchiveGroup`.
   - If a group does not exist in WinCC OA, report `SeverityError` at each JSON pointer:
     `archive group "FOO" does not exist in WinCC OA (available: EVENT, DEFAULT, ...)`

---

## 6. Provisioning `_archive` in `CreateInWinCCOa` (`create_b.go`)

In [`edge/internal/unsmodeler/create_b.go`](../../../internal/unsmodeler/create_b.go):

1. **Pass `*Settings` (and resolved archive group map `map[string]string`) into `PlanCreateB` and `CreateB`:**
   - In `mergedMeta(t *TypeProposal, d *Datapoint, s *Settings)`:
     Compute `m.Archive = effectiveArchive(s, te, de)` for every value element of the datapoint (note: when `s.OaArchiveEnabled` is true, even elements of a reused type or elements without explicit `unit`/`description` get an entry in `mergedMeta` so their `_archive` config is written!).
2. **`metaWriteCount(m elemMeta)`:**
   - Add:
     ```go
     if m.Archive != nil && m.Archive.Enabled && m.Archive.Group != "" {
         n++
     }
     ```
   - This ensures `UnsModelerCreatePlan.configWrites` and `UnsModelerObjectResult.configWrites` accurately reflect `_archive` config writes.
3. **`writeMeta` execution:**
   - Add `_archive` write block after `_alert_hdl`:
     ```go
     if m.Archive != nil && m.Archive.Enabled && m.Archive.Group != "" {
         const (
             archiveInfoType = 45 // DPCONFIG_DB_ARCHIVEINFO
             procValArch     = 15 // DPATTR_ARCH_PROC_VALARCH
         )
         groupDP := resolveArchiveDP(m.Archive.Group, archiveGroups)
         if err := api.DpSet(ctx,
             []string{dpe + ":_archive.._type", dpe + ":_archive.1._type"},
             []oahost.Value{
                 {Kind: oahost.KindInt, Int: archiveInfoType},
                 {Kind: oahost.KindInt, Int: procValArch},
             }, createTimeout); err != nil {
             errs = append(errs, fmt.Sprintf("%s _archive: %v", m.Path, err))
         } else if err := api.DpSet(ctx,
             []string{
                 dpe + ":_archive.1._proc",
                 dpe + ":_archive.1._class",
                 dpe + ":_archive.._archive",
             },
             []oahost.Value{
                 {Kind: oahost.KindInt, Int: procValArch},
                 {Kind: oahost.KindString, Str: local + ":" + groupDP + "."},
                 {Kind: oahost.KindBool, Bool: true},
             }, createTimeout); err != nil {
             errs = append(errs, fmt.Sprintf("%s _archive: %v", m.Path, err))
         } else {
             writes++
         }
     }
     ```

---

## 7. AI Assistant & Dashboard UI Changes

### 7.1 AI Assistant (`ai.go` & `direction_b.go`)
1. **New LLM Tool `listArchiveGroups` in `oaTools` (`edge/internal/unsmodeler/ai.go`):**
   - Description: `"Lists WinCC OA value archive groups (name, dpName, displayName, kind: NGA/HDB/RDB) available for _archive configuration."`
   - Calls `b.ArchiveGroups(ctx, a.System)`.
2. **System Prompt Update (`direction_b.go`):**
   - Update `promptB` `## Metadata (fixed set)` to include `archive` (`written to _archive with the selected WinCC OA archive group`).
   - Include the device's default archiving state in `## Rules` (e.g., if `s.OaArchiveEnabled` is true: `"Device default archiving is enabled with archive group %q; set element archive metadata only when a different group or enabled: false is needed."`).

### 7.2 Dashboard UI (`uns-modeler-detail.html`, `uns-modeler-detail.js`, `uns-onboarding.js`)
1. **Device Configuration Card (`uns-modeler-detail.html` & `uns-modeler-detail.js`):**
   - In the Direction B (`data-direction="MQTT_TO_WINCCOA"`) configuration grid:
     - Add checkbox `#dev-oaarchive-enabled`: **"Archive values in WinCC OA (`_archive`)"**
     - Add select/combobox `#dev-oaarchive-group`: **"WinCC OA Archive Group"** (populated asynchronously from `query { winccoa { archiveGroups { name dpName displayName kind } } }`, with fallback text input if `winccoa` browse is offline; disabled when `#dev-oaarchive-enabled` is unchecked).
2. **Direction B Proposal Cards (`uns-onboarding.js`):**
   - In `metaText(e)`: display `_archive (<group>)` when `e.archive && e.archive.enabled` (or `_archive off` when `e.archive && !e.archive.enabled`, or inherited badge when device-level `oaArchiveEnabled` is active).
   - On type and datapoint cards in `uns-onboarding.js`: allow toggling archiving or selecting the archive group per type/datapoint, updating the draft JSON and re-validating.

---

## 8. Milestones & Verification Plan

### Milestone 1: C++ Manager `opDpSet` Fix & `simhost` Archive Group Fixtures
- **Files:**
  - [`edge/winccoa/manager/MmqManager.cxx`](../../../winccoa/manager/MmqManager.cxx)
  - [`edge/internal/oahost/simhost/simhost.go`](../../../internal/oahost/simhost/simhost.go)
- **Tasks:**
  1. Update `MmqManager::opDpSet` (`MmqManager.cxx:1023-1034`) to check `attributeElementType(id)` before falling back to the DPE's value element type `et`, so setting `DPIDENTIFIER_VAR` (`_archive.1._class`) or `LANGTEXT_VAR` attributes on `DPELEMENT_TEXT` (`string`) DPEs decodes as `DpIdentifierVar` / `LangTextVar` rather than `TextVar`.
  2. Ensure `simhost` supports `_NGA_Group` / `_ValueArchive` datapoint fixtures in tests.

### Milestone 2: `winccoabrowse.ArchiveGroups` & UNS Modeler Backend (`schema`, `settings`, `validate_b`, `create_b`, `ai`)
- **Files:**
  - [`edge/internal/winccoabrowse/browse.go`](../../../internal/winccoabrowse/browse.go)
  - [`edge/internal/unsmodeler/schema/mapping.schema.json`](../../../internal/unsmodeler/schema/mapping.schema.json)
  - [`edge/internal/unsmodeler/document.go`](../../../internal/unsmodeler/document.go)
  - [`edge/internal/unsmodeler/settings.go`](../../../internal/unsmodeler/settings.go)
  - [`edge/internal/unsmodeler/validate.go`](../../../internal/unsmodeler/validate.go)
  - [`edge/internal/unsmodeler/validate_b.go`](../../../internal/unsmodeler/validate_b.go)
  - [`edge/internal/unsmodeler/create_b.go`](../../../internal/unsmodeler/create_b.go)
  - [`edge/internal/unsmodeler/ai.go`](../../../internal/unsmodeler/ai.go)
  - [`edge/internal/unsmodeler/direction_b.go`](../../../internal/unsmodeler/direction_b.go)
  - [`edge/internal/unsmodeler/b_test.go`](../../../internal/unsmodeler/b_test.go)
- **Tasks:**
  1. Implement `ArchiveGroups` and `ResolveArchiveGroup` in `winccoabrowse.Service`.
  2. Add `Archive` definition to `mapping.schema.json` and `document.go`, and `OaArchiveEnabled` / `OaArchiveGroup` to `Settings`.
  3. Implement validation in `settings.go` and `validate_b.go` (missing archive group, non-existent archive group in WinCC OA).
  4. Implement `_archive` config writes in `create_b.go` (`_archive.._type = 45`, `_archive.1._type = 15`, `_archive.1._proc = 15`, `_archive.1._class = "<local>:<groupDP>."`, `_archive.._archive = true`) and update `metaWriteCount`.
  5. Add `listArchiveGroups` tool in `ai.go` and update `promptB` in `direction_b.go`.
  6. Add unit tests in `b_test.go` and `winccoabrowse` verifying:
     - NGA (`_NGA_G_EVENT`, `_NGA_G_DEFAULT`, excluding `_NGA_G_ALERT` and `*_2`) and HDB (`_ValueArchive_0`, excluding deleted `state == 3`) discovery.
     - Device-level default archiving applied to all created DPEs.
     - Type-element and datapoint-element `archive` overrides (custom group and `enabled: false` opt-out).
     - Validation error when an unknown archive group is specified.

### Milestone 3: GraphQL Schema & Dashboard UI (After Human Approval of §3)
- **Files:**
  - [`edge/internal/graphql/schema/winccoa-browse.graphqls`](../../../internal/graphql/schema/winccoa-browse.graphqls)
  - [`edge/internal/graphql/schema/unsmodeler.graphqls`](../../../internal/graphql/schema/unsmodeler.graphqls)
  - [`edge/internal/graphql/resolvers/winccoa_browse.go`](../../../internal/graphql/resolvers/winccoa_browse.go)
  - [`edge/internal/graphql/resolvers/unsmodeler.go`](../../../internal/graphql/resolvers/unsmodeler.go)
  - [`dashboard/src/pages/uns-modeler-detail.html`](../../../../dashboard/src/pages/uns-modeler-detail.html)
  - [`dashboard/src/js/uns-modeler-detail.js`](../../../../dashboard/src/js/uns-modeler-detail.js)
  - [`dashboard/src/js/uns-onboarding.js`](../../../../dashboard/src/js/uns-onboarding.js)
  - [`edge/winccoa/doc/uns-modeler.md`](../../doc/uns-modeler.md)
- **Tasks:**
  1. Apply the approved GraphQL schema additions (§3), regenerate GraphQL models, and wire resolvers.
  2. Add the WinCC OA archiving toggle and archive group selector to the Direction B device settings and onboarding view in the dashboard, and rebuild `dashboard/dist/`.
  3. Update user documentation in `edge/winccoa/doc/uns-modeler.md`.
  4. Verify end-to-end against WinCC OA 3.21 (`Test321`) that created datapoints have `_archive` enabled with the selected `_NGA_G_*` group in PARA and record incoming MQTT values into NGA.

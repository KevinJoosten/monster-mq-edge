# Plan: Host Monitoring Feature

## Summary

To monitor the health of the host running `monster-mq-edge`, we need a lightweight, periodic background service that collects key host metrics (CPU usage, Memory utilization, Disk space) and publishes a single unified JSON payload to a configured MQTT topic.

The implementation must be:
- **Pure Go (Zero CGO)**: Supporting cross-compilation (e.g., to ARM for Raspberry Pi).
- **Zero Third-Party Dependencies**: Implementing native system calls and file parses for maximum performance, minimal dependency weight, and security.
- **Platform Parity**: Operating identically on Windows and Linux, with a fallback stub for other platforms (such as macOS/Darwin) to facilitate local development.

---

## Configuration

We will add a new `HostMonitoring` configuration block to `internal/config/config.go`, `yaml-json-schema.json`, and `config.yaml.example`.

### `config.yaml` Schema

```yaml
HostMonitoring:
  Enabled: false               # Disabled by default to keep resource usage minimal unless requested
  BaseTopic: "nodes/{NodeId}/host" # Destination topic; replaces {NodeId} or {node_id} at runtime
  IntervalSeconds: 60          # Collection & publish interval
  QoS: 0                       # QoS Level: 0, 1, or 2
```

---

## Architecture & Implementation

We will organize the code under a new internal package: `internal/hostinfo`.

### 1. Unified Metrics Model

We will define the metrics data structure inside `internal/hostinfo/stats.go`:

```go
package hostinfo

import "time"

type HostStats struct {
	Timestamp  string      `json:"timestamp"` // RFC3339 format
	CPUPercent float64     `json:"cpuPercent"`
	Memory     MemoryStats `json:"memory"`
	Disk       DiskStats   `json:"disk"`
}

type MemoryStats struct {
	Total       uint64  `json:"total"`       // Bytes
	Free        uint64  `json:"free"`        // Bytes
	Used        uint64  `json:"used"`        // Bytes
	UsedPercent float64 `json:"usedPercent"`
}

type DiskStats struct {
	Total       uint64  `json:"total"`       // Bytes
	Free        uint64  `json:"free"`        // Bytes
	Used        uint64  `json:"used"`        // Bytes
	UsedPercent float64 `json:"usedPercent"`
}
```

### 2. Platform-Specific Collectors

We will implement collectors per OS using Go build tags.

#### A. Linux (`monitor_linux.go`)
- **CPU**: Read `/proc/stat`. We will maintain the previous tick's total and idle CPU times in memory to calculate the delta-based CPU utilization percent on each interval.
- **Memory**: Read and parse `/proc/meminfo`. Specifically, we extract `MemTotal` and `MemAvailable` (falling back to `MemFree` + `Buffers` + `Cached` on older kernels).
- **Disk**: Use `syscall.Statfs` on the path of the working directory or root `/`.

#### B. Windows (`monitor_windows.go`)
- **CPU**: Use the `GetSystemTimes` Win32 API function dynamically loaded from `kernel32.dll` via `syscall.LazyDLL` to compute system times delta.
- **Memory**: Use the `GlobalMemoryStatusEx` Win32 API to fetch memory load and physical byte counts.
- **Disk**: Use `GetDiskFreeSpaceExW` Win32 API to compute free space for the path.

#### C. Fallback (`monitor_fallback.go` - Darwin/macOS)
- **CPU & Memory**: Return basic mock/placeholder numbers to compile and run on development machines.
- **Disk**: Use `syscall.Statfs` (matching the POSIX implementation of Linux) so disk space measurements remain accurate.

### 3. Server Integration

We will hook the service into the server lifecycle in `internal/broker/server.go`:

1. Define a `hostinfo.Collector` or a running loop in `Server.Serve()`.
2. Extract the configured `BaseTopic`, resolving the `{NodeId}`/`{node_id}` template with the broker's active `cfg.NodeID`.
3. Periodically collect metrics, serialize them to JSON, and call `server.Publish(topic, payload, false, qos)`.
4. Register a clean stop mechanism in `Server.Close()` to terminate the goroutine ticker loop.

---

## Test Plan

1. **Config Validation**:
   - Ensure the parser rejects invalid QoS levels (>2 or <0) or invalid intervals (<=0).
   - Verify proper fallback defaults.
2. **Platform Compilation**:
   - Run `GOOS=linux go build` and `GOOS=windows go build` to ensure both compile correctly without CGO.
3. **Collector Verification**:
   - Assert that the fallback collector correctly populates the disk sizes and provides default CPU/memory values.
4. **Integration Test**:
   - Run a test in `test/integration/host_monitoring_test.go`.
   - Start the broker with `HostMonitoring` enabled, set the interval to 1 second, subscribe to the target topic, and assert that JSON payloads containing valid keys and numerical values are periodically received.

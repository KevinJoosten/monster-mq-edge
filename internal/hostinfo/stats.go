package hostinfo

type HostStats struct {
	Timestamp string      `json:"timestamp"` // RFC3339 format
	CPU       CPUStats    `json:"cpu"`
	Memory    MemoryStats `json:"memory"`
	Disk      DiskStats   `json:"disk"`
}

type CPUStats struct {
	Host   float64 `json:"host"`
	Broker float64 `json:"broker"`
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

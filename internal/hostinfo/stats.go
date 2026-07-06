package hostinfo

type HostStats struct {
	Timestamp         string      `json:"timestamp"` // RFC3339 format
	CPUPercent        float64     `json:"cpuPercent"`
	ProcessCPUPercent float64     `json:"processCpuPercent"`
	Memory            MemoryStats `json:"memory"`
	Disk              DiskStats   `json:"disk"`
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

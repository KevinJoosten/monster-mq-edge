//go:build !linux && !windows

package hostinfo

import (
	"time"
)

type SystemMonitor struct{}

func NewSystemMonitor() *SystemMonitor {
	return &SystemMonitor{}
}

func (sm *SystemMonitor) GetStats() (HostStats, error) {
	return HostStats{
		Timestamp:         time.Now().Format(time.RFC3339),
		CPUPercent:        0.0,
		ProcessCPUPercent: 0.0,
		Memory: MemoryStats{
			Total:       16106127360, // 15 GB placeholder
			Free:        8053063680,  // 7.5 GB placeholder
			Used:        8053063680,  // 7.5 GB placeholder
			UsedPercent: 50.0,
		},
		Disk: DiskStats{
			Total:       256000000000, // 256 GB placeholder
			Free:        128000000000, // 128 GB placeholder
			Used:        128000000000, // 128 GB placeholder
			UsedPercent: 50.0,
		},
	}, nil
}

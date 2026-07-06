//go:build !linux && !windows

package hostinfo

import (
	"runtime"
	"syscall"
	"time"
)

type SystemMonitor struct {
	lastProcessCPUTime    time.Duration
	lastProcessSampleTime time.Time
}

func NewSystemMonitor() *SystemMonitor {
	sm := &SystemMonitor{}
	_, _ = sm.readProcessCPU() // initial read to set baselines
	return sm
}

func (sm *SystemMonitor) GetStats() (HostStats, error) {
	procCPU, err := sm.readProcessCPU()
	if err != nil {
		procCPU = 0
	}

	return HostStats{
		Timestamp: time.Now().Format(time.RFC3339),
		CPU: CPUStats{
			Host:   0.0, // Omitted on macOS/fallback
			Broker: procCPU,
		},
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

func (sm *SystemMonitor) readProcessCPU() (float64, error) {
	var ru syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	if err != nil {
		return 0, err
	}

	now := time.Now()
	user := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	sys := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	total := user + sys

	if sm.lastProcessSampleTime.IsZero() {
		sm.lastProcessCPUTime = total
		sm.lastProcessSampleTime = now
		return 0, nil
	}

	timeDelta := now.Sub(sm.lastProcessSampleTime)
	cpuDelta := total - sm.lastProcessCPUTime

	sm.lastProcessCPUTime = total
	sm.lastProcessSampleTime = now

	if timeDelta <= 0 {
		return 0, nil
	}

	numCPU := float64(runtime.NumCPU())
	percent := (float64(cpuDelta) / float64(timeDelta)) * 100.0
	if percent < 0 {
		percent = 0
	}
	maxPercent := 100.0 * numCPU
	if percent > maxPercent {
		percent = maxPercent
	}
	return percent, nil
}

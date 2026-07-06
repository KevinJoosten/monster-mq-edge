//go:build linux

package hostinfo

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type SystemMonitor struct {
	lastCPUUser    uint64
	lastCPUNice    uint64
	lastCPUSystem  uint64
	lastCPUIdle    uint64
	lastCPUIowait  uint64
	lastCPUIrq     uint64
	lastCPUSoftirq        uint64
	lastCPUSteal          uint64
	lastProcessCPUTime    time.Duration
	lastProcessSampleTime time.Time
	lastDiskStats         DiskStats
	lastDiskTime          time.Time
}

func NewSystemMonitor() *SystemMonitor {
	sm := &SystemMonitor{}
	_, _ = sm.readCPU()        // initial read to set baselines
	_, _ = sm.readProcessCPU() // initial read to set baselines
	return sm
}

func (sm *SystemMonitor) GetStats() (HostStats, error) {
	cpu, err := sm.readCPU()
	if err != nil {
		cpu = 0
	}

	procCPU, err := sm.readProcessCPU()
	if err != nil {
		procCPU = 0
	}

	mem, err := sm.readMemory()
	if err != nil {
		mem = MemoryStats{}
	}

	disk, err := sm.readDisk()
	if err != nil {
		disk = DiskStats{}
	}

	return HostStats{
		Timestamp:         time.Now().Format(time.RFC3339),
		CPUPercent:        cpu,
		ProcessCPUPercent: procCPU,
		Memory:            mem,
		Disk:              disk,
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

func (sm *SystemMonitor) readCPU() (float64, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return 0, fmt.Errorf("empty /proc/stat")
	}
	line := scanner.Text()
	if !strings.HasPrefix(line, "cpu ") {
		return 0, fmt.Errorf("invalid format in /proc/stat")
	}

	fields := strings.Fields(line)[1:]
	if len(fields) < 8 {
		return 0, fmt.Errorf("insufficient cpu fields in /proc/stat")
	}

	var values [8]uint64
	for i := 0; i < 8; i++ {
		val, err := strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			return 0, err
		}
		values[i] = val
	}

	user, nice, sys, idle, iowait, irq, softirq, steal := values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]

	idleTime := idle + iowait
	nonIdleTime := user + nice + sys + irq + softirq + steal
	totalTime := idleTime + nonIdleTime

	lastIdle := sm.lastCPUIdle + sm.lastCPUIowait
	lastNonIdle := sm.lastCPUUser + sm.lastCPUNice + sm.lastCPUSystem + sm.lastCPUIrq + sm.lastCPUSoftirq + sm.lastCPUSteal
	lastTotal := lastIdle + lastNonIdle

	sm.lastCPUUser = user
	sm.lastCPUNice = nice
	sm.lastCPUSystem = sys
	sm.lastCPUIdle = idle
	sm.lastCPUIowait = iowait
	sm.lastCPUIrq = irq
	sm.lastCPUSoftirq = softirq
	sm.lastCPUSteal = steal

	if lastTotal == 0 {
		return 0, nil
	}

	totalDelta := totalTime - lastTotal
	if totalDelta == 0 {
		return 0, nil
	}

	idleDelta := idleTime - lastIdle
	if totalDelta < idleDelta {
		return 0, nil
	}

	usedDelta := totalDelta - idleDelta
	return (float64(usedDelta) / float64(totalDelta)) * 100.0, nil
}

func (sm *SystemMonitor) readMemory() (MemoryStats, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return MemoryStats{}, err
	}
	defer file.Close()

	var memTotal, memAvailable, memFree, buffers, cached uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		val, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		valBytes := val * 1024

		switch key {
		case "MemTotal":
			memTotal = valBytes
		case "MemAvailable":
			memAvailable = valBytes
		case "MemFree":
			memFree = valBytes
		case "Buffers":
			buffers = valBytes
		case "Cached":
			cached = valBytes
		}
	}

	if memTotal == 0 {
		return MemoryStats{}, fmt.Errorf("unable to read MemTotal")
	}

	if memAvailable == 0 {
		memAvailable = memFree + buffers + cached
	}

	used := memTotal - memAvailable
	var usedPercent float64
	if memTotal > 0 {
		usedPercent = (float64(used) / float64(memTotal)) * 100.0
	}

	return MemoryStats{
		Total:       memTotal,
		Free:        memAvailable,
		Used:        used,
		UsedPercent: usedPercent,
	}, nil
}

func (sm *SystemMonitor) readDisk() (DiskStats, error) {
	now := time.Now()
	if !sm.lastDiskTime.IsZero() && now.Sub(sm.lastDiskTime) < 30*time.Second {
		return sm.lastDiskStats, nil
	}

	var stat syscall.Statfs_t
	err := syscall.Statfs("/", &stat)
	if err != nil {
		return DiskStats{}, err
	}

	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	used := total - free
	var usedPercent float64
	if total > 0 {
		usedPercent = (float64(used) / float64(total)) * 100.0
	}

	stats := DiskStats{
		Total:       total,
		Free:        free,
		Used:        used,
		UsedPercent: usedPercent,
	}

	sm.lastDiskStats = stats
	sm.lastDiskTime = now
	return stats, nil
}

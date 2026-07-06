//go:build windows

package hostinfo

import (
	"os"
	"runtime"
	"syscall"
	"time"
	"unsafe"
)

type SystemMonitor struct {
	lastIdleTime          uint64
	lastKernelTime        uint64
	lastUserTime          uint64
	lastProcessCPUTime    time.Duration
	lastProcessSampleTime time.Time
	lastDiskStats         DiskStats
	lastDiskTime          time.Time
}

type FILETIME struct {
	LowDateTime  uint32
	HighDateTime uint32
}

type MEMORYSTATUSEX struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	procGetSystemTimes       = kernel32.NewProc("GetSystemTimes")
	procGlobalMemoryStatusEx = kernel32.NewProc("GlobalMemoryStatusEx")
	procGetDiskFreeSpaceExW  = kernel32.NewProc("GetDiskFreeSpaceExW")
	procGetProcessTimes      = kernel32.NewProc("GetProcessTimes")
	procGetCurrentProcess    = kernel32.NewProc("GetCurrentProcess")
)

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
	hProcess, _, _ := procGetCurrentProcess.Call()

	var creation, exit, kernel, user FILETIME
	r1, _, err := procGetProcessTimes.Call(
		hProcess,
		uintptr(unsafe.Pointer(&creation)),
		uintptr(unsafe.Pointer(&exit)),
		uintptr(unsafe.Pointer(&kernel)),
		uintptr(unsafe.Pointer(&user)),
	)
	if r1 == 0 {
		return 0, err
	}

	kernelTime := (uint64(kernel.HighDateTime) << 32) | uint64(kernel.LowDateTime)
	userTime := (uint64(user.HighDateTime) << 32) | uint64(user.LowDateTime)
	total := time.Duration(kernelTime+userTime) * 100 * time.Nanosecond

	now := time.Now()

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
	var idle, kernel, user FILETIME
	r1, _, err := procGetSystemTimes.Call(
		uintptr(unsafe.Pointer(&idle)),
		uintptr(unsafe.Pointer(&kernel)),
		uintptr(unsafe.Pointer(&user)),
	)
	if r1 == 0 {
		return 0, err
	}

	idleTime := (uint64(idle.HighDateTime) << 32) | uint64(idle.LowDateTime)
	kernelTime := (uint64(kernel.HighDateTime) << 32) | uint64(kernel.LowDateTime)
	userTime := (uint64(user.HighDateTime) << 32) | uint64(user.LowDateTime)

	lastIdle := sm.lastIdleTime
	lastKernel := sm.lastKernelTime
	lastUser := sm.lastUserTime

	sm.lastIdleTime = idleTime
	sm.lastKernelTime = kernelTime
	sm.lastUserTime = userTime

	if lastKernel == 0 && lastUser == 0 {
		return 0, nil
	}

	idleDelta := idleTime - lastIdle
	kernelDelta := kernelTime - lastKernel
	userDelta := userTime - lastUser

	totalDelta := kernelDelta + userDelta
	if totalDelta == 0 {
		return 0, nil
	}

	var busyDelta uint64
	if totalDelta >= idleDelta {
		busyDelta = totalDelta - idleDelta
	}

	return (float64(busyDelta) / float64(totalDelta)) * 100.0, nil
}

func (sm *SystemMonitor) readMemory() (MemoryStats, error) {
	var ms MEMORYSTATUSEX
	ms.Length = uint32(unsafe.Sizeof(ms))
	r1, _, err := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&ms)))
	if r1 == 0 {
		return MemoryStats{}, err
	}

	total := ms.TotalPhys
	free := ms.AvailPhys
	used := total - free
	var usedPercent float64
	if total > 0 {
		usedPercent = (float64(used) / float64(total)) * 100.0
	}

	return MemoryStats{
		Total:       total,
		Free:        free,
		Used:        used,
		UsedPercent: usedPercent,
	}, nil
}

func (sm *SystemMonitor) readDisk() (DiskStats, error) {
	now := time.Now()
	if !sm.lastDiskTime.IsZero() && now.Sub(sm.lastDiskTime) < 30*time.Second {
		return sm.lastDiskStats, nil
	}

	drive := os.Getenv("SystemDrive")
	if drive == "" {
		drive = "C:"
	}
	drive += "\\"

	pathPtr, err := syscall.UTF16PtrFromString(drive)
	if err != nil {
		return DiskStats{}, err
	}

	var freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes uint64
	r1, _, err := procGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
	)
	if r1 == 0 {
		return DiskStats{}, err
	}

	total := totalNumberOfBytes
	free := totalNumberOfFreeBytes
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

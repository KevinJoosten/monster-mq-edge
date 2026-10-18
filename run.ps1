<#
.SYNOPSIS
    MonsterMQ Edge run script (Go broker) - Windows PowerShell.

.DESCRIPTION
    Starts the MonsterMQ Edge broker, with options to build or compile beforehand,
    kill running instances, and pass broker configuration flags.

.PARAMETER Build
    Build the binary (CGO_ENABLED=0) before starting (-b, -build).

.PARAMETER Compile
    Type-check / compile all packages (no binary output) (-c, -compile).

.PARAMETER Norun
    Don't start the broker (combine with -b/-c for build-only) (-n, -norun).

.PARAMETER Nokill
    Don't kill any already-running monstermq-edge first (-nk, -nokill).

.PARAMETER Help
    Show this help message (-h, -help, --help).
#>

[CmdletBinding()]
param(
    [Alias("b")]
    [switch]$Build,

    [Alias("c")]
    [switch]$Compile,

    [Alias("n")]
    [switch]$Norun,

    [Alias("nk")]
    [switch]$Nokill,

    [Alias("h", "?")]
    [switch]$Help,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

function Show-Help {
    Write-Host "MonsterMQ Edge run script (Go broker)."
    Write-Host "Usage: .\run.ps1 [SCRIPT_OPTIONS] [-- BROKER_OPTIONS]"
    Write-Host ""
    Write-Host "Script options (before --):"
    Write-Host "  -b,  -build       Build the binary (CGO_ENABLED=0) before starting"
    Write-Host "  -c,  -compile     Type-check / compile all packages (no binary output)"
    Write-Host "  -n,  -norun       Don't start the broker (combine with -b/-c for build-only)"
    Write-Host "  -nk, -nokill      Don't kill any already-running monstermq-edge first"
    Write-Host "  -h,  -help        Show this help"
    Write-Host ""
    Write-Host "Broker options (after --):"
    Write-Host "  Anything after -- is passed straight to the binary."
    Write-Host "  See .\bin\monstermq-edge.exe -h for available flags (-config, -log-level, -version)."
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\run.ps1                       Start with config.yaml (or config.yaml.example)"
    Write-Host "  .\run.ps1 -b                    Build, then start"
    Write-Host "  .\run.ps1 -b -n                 Build only"
    Write-Host "  .\run.ps1 -b -- -config foo.yaml"
    exit 0
}

$Bin = "bin\monstermq-edge.exe"
$BrokerArgs = [System.Collections.Generic.List[string]]::new()
$SawSep = $false

if ($ExtraArgs) {
    foreach ($arg in $ExtraArgs) {
        if ($SawSep) {
            $BrokerArgs.Add($arg)
            continue
        }
        switch ($arg) {
            "--"          { $SawSep = $true }
            "-b"          { $Build = $true }
            "-build"      { $Build = $true }
            "--build"     { $Build = $true }
            "-c"          { $Compile = $true }
            "-compile"    { $Compile = $true }
            "--compile"   { $Compile = $true }
            "-n"          { $Norun = $true }
            "-norun"      { $Norun = $true }
            "--norun"     { $Norun = $true }
            "-nk"         { $Nokill = $true }
            "-nokill"     { $Nokill = $true }
            "--nokill"    { $Nokill = $true }
            "-h"          { Show-Help }
            "-help"       { Show-Help }
            "--help"      { Show-Help }
            "-?"          { Show-Help }
            default       { $BrokerArgs.Add($arg) }
        }
    }
}

if ($Help) {
    Show-Help
}

Push-Location $PSScriptRoot
try {
    if ($Build) {
    if (-not (Test-Path "dashboard")) {
        if (Test-Path "..\dashboard") {
            Write-Host "Symlink 'dashboard' not found. Creating junction to ..\dashboard..."
            try {
                New-Item -ItemType Junction -Path "dashboard" -Target "..\dashboard" | Out-Null
            } catch {
                cmd.exe /c "mklink /D dashboard ..\dashboard 2>nul || mklink /J dashboard ..\dashboard 2>nul" | Out-Null
            }
        } else {
            [Console]::Error.WriteLine("Error: Dashboard not found! Neither ./dashboard nor ../dashboard exists.")
            exit 1
        }
    }

    if (-not (Test-Path "dashboard\dist")) {
        Write-Host "Building dashboard..."
        Push-Location "dashboard"
        try {
            & npm run build
            if ($LASTEXITCODE -ne 0) {
                [Console]::Error.WriteLine("Error: Failed to build dashboard.")
                exit 1
            }
        } finally {
            Pop-Location
        }
    }

    Write-Host "Syncing dashboard from dashboard/dist..."
    if (-not (Test-Path "internal\dashboard\dist")) {
        New-Item -ItemType Directory -Force -Path "internal\dashboard\dist" | Out-Null
    }
    Copy-Item -Path "dashboard\dist\*" -Destination "internal\dashboard\dist\" -Recurse -Force

    Write-Host "Building monstermq-edge..."
    if (-not (Test-Path "bin")) {
        New-Item -ItemType Directory -Force -Path "bin" | Out-Null
    }

    $env:CGO_ENABLED = "0"
    $version = ""
    if (Test-Path "version.txt") {
        $version = (Get-Content "version.txt" -Raw).Trim()
    }
    $ldflags = "-s -w -X monstermq.io/edge/internal/version.Version=$version"
    & go build -trimpath "-ldflags=$ldflags" -o $Bin .\cmd\monstermq-edge
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("Build failed")
        exit 1
    }
    Write-Host "Built: $Bin"
}

if ($Compile -and -not $Build) {
    Write-Host "Compiling all packages..."
    & go build ./...
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("Compilation failed")
        exit 1
    }
}

if ($Norun) {
    exit 0
}

if (-not (Test-Path $Bin)) {
    [Console]::Error.WriteLine("Binary $Bin not found. Run with -b first.")
    exit 1
}

if (-not $Nokill) {
    $existing = Get-Process -Name "monstermq-edge" -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "Killing existing instances of monstermq-edge..."
        Stop-Process -Name "monstermq-edge" -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
    }
}

# Pick a config: explicit -config wins; otherwise prefer config.yaml then the example.
$hasConfig = $false
foreach ($arg in $BrokerArgs) {
    if ($arg -eq "-config" -or $arg -like "-config=*") {
        $hasConfig = $true
        break
    }
}

if (-not $hasConfig) {
    if (Test-Path "config.yaml") {
        $BrokerArgs.Insert(0, "config.yaml")
        $BrokerArgs.Insert(0, "-config")
    } elseif (Test-Path "config.yaml.example") {
        $BrokerArgs.Insert(0, "config.yaml.example")
        $BrokerArgs.Insert(0, "-config")
    }
}

    Write-Host "Starting: $Bin $($BrokerArgs -join ' ')"
    & ".\$Bin" @BrokerArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

<#
.SYNOPSIS
    Master build script for MonsterMQ Edge (Windows).

.DESCRIPTION
    Builds MonsterMQ Edge artifacts on Windows: native Go binary, Debian packages
    (via bash and scripts/build-deb.sh), and local Docker image. Optionally cleans
    build outputs and triggers release publication.

.PARAMETER All
    Build all artifacts (binary, debian packages, docker image). This is the default.

.PARAMETER Binary
    Build native Go binary for current machine.

.PARAMETER Deb
    Build Debian packages for all target architectures (arm64, armhf, amd64).

.PARAMETER Docker
    Build local Docker image (native platform).

.PARAMETER Publish
    Trigger release publication script after building.

.PARAMETER Clean
    Clean build output directories (bin and dist).

.PARAMETER Help
    Show this help message.

.EXAMPLE
    .\build.ps1
    Build all artifacts locally (binary, debian packages, docker image).

.EXAMPLE
    .\build.ps1 -Binary
    Build native Go binary for current machine.

.EXAMPLE
    .\build.ps1 -Clean -Binary
    Clean build output directories and build native Go binary.

.EXAMPLE
    .\build.ps1 -Deb
    Build Debian packages for all target architectures.
#>

[CmdletBinding()]
param(
    [switch]$All,

    [switch]$Binary,

    [switch]$Deb,

    [switch]$Docker,

    [Alias("p")]
    [switch]$Publish,

    [switch]$Clean,

    [Alias("h", "?")]
    [switch]$Help,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$ErrorActionPreference = "Stop"

function Show-Usage {
    param([int]$ExitCode = 0)
    Write-Host "Usage: build.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  --all            Build all artifacts (default)"
    Write-Host "  --binary         Build native Go binary for current machine"
    Write-Host "  --deb            Build Debian packages (arm64, armhf, amd64)"
    Write-Host "  --docker         Build local Docker image (native platform)"
    Write-Host "  -p, --publish    Trigger ./publish.sh after building"
    Write-Host "  --clean          Clean build output directories"
    Write-Host "  -h, --help       Show this help message"
    Write-Host ""
    exit $ExitCode
}

# Parse any additional GNU-style or unrecognized arguments passed via ExtraArgs
if ($ExtraArgs) {
    foreach ($arg in $ExtraArgs) {
        switch -Regex ($arg) {
            '^--?all$'           { $All = $true }
            '^--?binary$'        { $Binary = $true }
            '^--?deb$'           { $Deb = $true }
            '^--?docker$'        { $Docker = $true }
            '^(-p|--?publish)$'  { $Publish = $true }
            '^--?clean$'         { $Clean = $true }
            '^(-h|--?help|-\?)$' { $Help = $true }
            default {
                [Console]::Error.WriteLine("Unknown option: $arg")
                Show-Usage 1
            }
        }
    }
}

if ($Help) {
    Show-Usage
}

# Resolve repository directory
$RepoDir = $PSScriptRoot
Set-Location $RepoDir

$Bin = "bin\monstermq-edge.exe"
$Pkg = ".\cmd\monstermq-edge"

# If no specific target was selected, default to building all (unless only cleaning)
if (-not ($Binary -or $Deb -or $Docker -or $Clean)) {
    $Binary = $true
    $Deb = $true
    $Docker = $true
} elseif ($All) {
    $Binary = $true
    $Deb = $true
    $Docker = $true
}

# Read version
$versionFile = "$RepoDir\version.txt"
if (-not (Test-Path $versionFile)) {
    [Console]::Error.WriteLine("Error: version.txt not found")
    exit 1
}
$Version = (Get-Content $versionFile -Raw).Trim()

Write-Host "=== MonsterMQ Edge Build Pipeline (v$Version) ==="

# Clean build output directories
if ($Clean) {
    Write-Host "Cleaning build directories..."
    if (Test-Path "$RepoDir\bin") {
        Remove-Item -Recurse -Force "$RepoDir\bin"
    }
    if (Test-Path "$RepoDir\dist") {
        Remove-Item -Recurse -Force "$RepoDir\dist"
    }
    Write-Host "Clean complete."
}

# [1/3] Native Go binary
if ($Binary) {
    Write-Host "[1/3] Building native Go binary..."
    if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
        [Console]::Error.WriteLine("go was not found on PATH.")
        exit 1
    }
    if (-not (Test-Path "$RepoDir\bin")) {
        New-Item -ItemType Directory -Force -Path "$RepoDir\bin" | Out-Null
    }
    $env:CGO_ENABLED = "0"
    $ldflags = "-s -w -X monstermq.io/edge/internal/version.Version=$Version"
    & go build -trimpath "-ldflags=$ldflags" -o $Bin $Pkg
    if ($LASTEXITCODE -ne 0) {
        [Console]::Error.WriteLine("Build failed")
        exit 1
    }
    Write-Host "Native binary built at: $Bin"
}

# [2/3] Debian packages
if ($Deb) {
    Write-Host "[2/3] Building Debian packages (arm64, armhf, amd64)..."
    $bashExe = $null
    $gitBash = "$env:ProgramFiles\Git\bin\bash.exe"
    if (Test-Path $gitBash) {
        $bashExe = $gitBash
    } else {
        $cmd = Get-Command bash -ErrorAction SilentlyContinue
        if ($cmd) {
            $bashExe = $cmd.Source
        }
    }

    if (-not $bashExe) {
        [Console]::Error.WriteLine("--deb needs bash to run scripts\build-deb.sh, but none was found.")
        Write-Host "Skipping Debian packages on this machine."
    } else {
        & $bashExe -c "command -v ar >/dev/null 2>&1"
        if ($LASTEXITCODE -ne 0) {
            [Console]::Error.WriteLine("ar (binutils) not found in bash environment. Skipping Debian packaging.")
        } else {
            foreach ($arch in @("arm64", "armhf", "amd64")) {
                Write-Host "  -> $arch"
                & $bashExe ./scripts/build-deb.sh --arch $arch
                if ($LASTEXITCODE -ne 0) {
                    [Console]::Error.WriteLine("Debian package build failed for $arch")
                    exit 1
                }
            }
            Write-Host "Debian packages built in bin/"
        }
    }
}

# [3/3] Local Docker image
if ($Docker) {
    Write-Host "[3/3] Building local Docker image..."
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        [Console]::Error.WriteLine("docker was not found on PATH. Skipping Docker build.")
    } else {
        & docker build -t rocworks/monstermq-edge:latest -f docker/Dockerfile .
        if ($LASTEXITCODE -ne 0) {
            [Console]::Error.WriteLine("Docker build failed")
            exit 1
        }
        Write-Host "Local Docker image built: rocworks/monstermq-edge:latest"
    }
}

Write-Host "=== Build Complete ==="

# Trigger release publication
if ($Publish) {
    Write-Host "Triggering release publication..."
    if (Test-Path "$RepoDir\publish.ps1") {
        & "$RepoDir\publish.ps1"
    } elseif (Test-Path "$RepoDir\publish.bat") {
        & cmd.exe /c "$RepoDir\publish.bat"
    } else {
        $bashExe = $null
        $gitBash = "$env:ProgramFiles\Git\bin\bash.exe"
        if (Test-Path $gitBash) {
            $bashExe = $gitBash
        } else {
            $cmd = Get-Command bash -ErrorAction SilentlyContinue
            if ($cmd) {
                $bashExe = $cmd.Source
            }
        }
        if ($bashExe) {
            & $bashExe ./publish.sh
        } else {
            [Console]::Error.WriteLine("Cannot run publish.sh - bash not found.")
        }
    }
}

exit 0

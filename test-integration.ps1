<#
.SYNOPSIS
    Builds and runs MonsterMQ Edge integration tests.

.DESCRIPTION
    Builds the integration tests once to bin\integration.test.exe and runs them from there.
    The Windows firewall remembers "Allow access" per program path. Plain `go test`
    builds into a new temp folder on every run, so it asks every time.

.EXAMPLE
    .\test-integration.ps1 -test.run TestPubSub -test.v
#>

[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$TestArgs
)

Push-Location $PSScriptRoot
try {
    if (-not (Test-Path "bin")) {
        New-Item -ItemType Directory -Force -Path "bin" | Out-Null
    }

    & go test -c -o bin\integration.test.exe .\test\integration
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    Push-Location "test\integration"
    try {
        $runArgs = @("-test.count=1", "-test.timeout=300s")
        if ($TestArgs) {
            $runArgs += $TestArgs
        }
        & "..\..\bin\integration.test.exe" @runArgs
        $rc = $LASTEXITCODE
    } finally {
        Pop-Location
    }

    exit $rc
} finally {
    Pop-Location
}

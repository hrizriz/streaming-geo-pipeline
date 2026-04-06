$Root = Split-Path -Parent $PSScriptRoot
Push-Location (Join-Path $Root "python")
try {
    & python -m stack_health_agent @args
} finally {
    Pop-Location
}

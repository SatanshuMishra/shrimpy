<#
.SYNOPSIS
    Development utility script for Shrimpy Discord Bot

.DESCRIPTION
    Provides commands to manage the bot, worker, and Redis services.
    Uses PID files and process-based lifecycle so start/stop/status work
    reliably across sessions and invocations (e.g. powershell -File).

.PARAMETER Action
    The action to perform: start, stop, restart, status, logs, flush, worker-logs

.EXAMPLE
    .\scripts\dev.ps1 start
    .\scripts\dev.ps1 stop
    .\scripts\dev.ps1 restart
    .\scripts\dev.ps1 status
    .\scripts\dev.ps1 logs
    .\scripts\dev.ps1 worker-logs
    .\scripts\dev.ps1 flush
#>

param(
    [Parameter(Position=0)]
    [ValidateSet("start", "start-sync", "stop", "restart", "status", "logs", "worker-logs", "flush", "help")]
    [string]$Action = "help"
)

# Configuration
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$RedisPort = if ($env:REDIS_PORT) { $env:REDIS_PORT } else { "6380" }
$RedisContainer = "redis-shrimpy"
$RedisPassword = "shrimpy"

# PID and log paths (process-based lifecycle; survives session boundaries)
$GeneratedDir = Join-Path $ProjectRoot "generated"
$PidFileBot = Join-Path $GeneratedDir "shrimpy-bot.pid"
$PidFileWorker = Join-Path $GeneratedDir "shrimpy-worker.pid"
$LogFileBot = Join-Path $GeneratedDir "shrimpy-bot.log"
$LogFileBotErr = Join-Path $GeneratedDir "shrimpy-bot.err"
$LogFileWorker = Join-Path $GeneratedDir "shrimpy-worker.log"
$LogFileWorkerErr = Join-Path $GeneratedDir "shrimpy-worker.err"

function Write-ColorOutput($ForegroundColor, $Message) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $ForegroundColor
    Write-Output $Message
    $host.UI.RawUI.ForegroundColor = $fc
}

function Ensure-GeneratedDir {
    if (-not (Test-Path $GeneratedDir)) {
        New-Item -ItemType Directory -Path $GeneratedDir -Force | Out-Null
    }
}

# Returns the command line of a process by PID (WMI/CIM); $null if not found or inaccessible.
function Get-ProcessCommandLine($ProcessId) {
    $p = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue
    if ($p) { $p.CommandLine } else { $null }
}

# True if the process with $ProcessId is our bot or worker (command line contains project path and script name).
function Test-IsOurProcess($ProcessId, $ScriptName) {
    $cmd = Get-ProcessCommandLine $ProcessId
    if (-not $cmd) { return $false }
    $escapedRoot = [Management.Automation.WildcardPattern]::Escape($ProjectRoot)
    $cmd -like "*$escapedRoot*" -and $cmd -like "*$ScriptName*"
}

# Stop process by PID file; remove PID file afterward.
function Stop-ProcessByPidFile($PidFilePath, $Label) {
    if (-not (Test-Path $PidFilePath)) { return }
    $pidVal = Get-Content $PidFilePath -Raw -ErrorAction SilentlyContinue
    if (-not $pidVal) {
        Remove-Item $PidFilePath -Force -ErrorAction SilentlyContinue
        return
    }
    $pidVal = $pidVal.Trim()
    $proc = Get-Process -Id $pidVal -ErrorAction SilentlyContinue
    if ($proc) {
        Stop-Process -Id $pidVal -Force -ErrorAction SilentlyContinue
        Write-ColorOutput Yellow "[OK] $Label stopped (PID $pidVal)"
    }
    Remove-Item $PidFilePath -Force -ErrorAction SilentlyContinue
}

# Kill any orphan process whose command line contains $ProjectRoot and $ScriptName.
function Stop-OrphanProcesses($ScriptName, $Label) {
    $escapedRoot = [Management.Automation.WildcardPattern]::Escape($ProjectRoot)
    $procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and $_.CommandLine -like "*$escapedRoot*" -and $_.CommandLine -like "*$ScriptName*" }
    foreach ($p in $procs) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        Write-ColorOutput Yellow "[OK] $Label stopped (orphan PID $($p.ProcessId))"
    }
}

# Idempotent stop: by PID file first, then by process identity (orphans).
function Ensure-StoppedBot {
    Stop-ProcessByPidFile $PidFileBot "Bot"
    Stop-OrphanProcesses "run.py" "Bot"
}

function Ensure-StoppedWorker {
    Stop-ProcessByPidFile $PidFileWorker "Worker"
    Stop-OrphanProcesses "worker.py" "Worker"
}

function Start-Redis {
    $status = docker ps -a --filter "name=$RedisContainer" --format "{{.Status}}" 2>$null
    if ($status -like "Up*") {
        Write-ColorOutput Green "[OK] Redis already running"
        return $true
    }

    if ($status) {
        Write-Output "[...] Starting Redis container..."
        docker start $RedisContainer | Out-Null
    } else {
        Write-Output "[...] Creating Redis container..."
        # SECURITY: Bind to localhost only (127.0.0.1) to prevent LAN/WAN exposure
        # Redis with RQ uses pickle serialization; exposed Redis = potential RCE
        docker run -d --name $RedisContainer -p "127.0.0.1:${RedisPort}:6379" redis:latest redis-server --requirepass $RedisPassword | Out-Null
    }

    Start-Sleep 2
    $status = docker ps --filter "name=$RedisContainer" --format "{{.Status}}" 2>$null
    if ($status -like "Up*") {
        Write-ColorOutput Green "[OK] Redis started on port $RedisPort"
        return $true
    } else {
        Write-ColorOutput Red "[FAIL] Redis failed to start"
        return $false
    }
}

function Stop-Services {
    Write-Output "[...] Stopping services..."
    Ensure-StoppedBot
    Ensure-StoppedWorker
}

function Start-Services {
    Ensure-GeneratedDir

    # Idempotent start: ensure no existing process, then start one.
    Ensure-StoppedBot
    Ensure-StoppedWorker

    if (-not (Start-Redis)) {
        Write-ColorOutput Red "[FAIL] Cannot start services without Redis"
        return
    }

    $pythonExe = Join-Path $ProjectRoot "venv\Scripts\python.exe"
    if (-not (Test-Path $pythonExe)) {
        Write-ColorOutput Red "[FAIL] venv not found at $ProjectRoot\venv"
        return
    }

    # Start Bot (stdout/stderr redirected to generated logs)
    Write-Output "[...] Starting bot..."
    $env:REDIS_PORT = $RedisPort
    $env:REDIS_PASSWORD = $RedisPassword
    $env:REDIS_HOST = "localhost"
    $botArgs = @("bot/run.py")
    if ($env:SYNC_COMMANDS) {
        $botArgs += "--sync"
        Write-Output "[...] Command tree will sync on startup (SYNC_COMMANDS set)"
    }
    $botProcess = Start-Process -FilePath $pythonExe -ArgumentList $botArgs -WorkingDirectory $ProjectRoot `
        -PassThru -NoNewWindow `
        -RedirectStandardOutput $LogFileBot -RedirectStandardError $LogFileBotErr
    $botProcess.Id | Set-Content $PidFileBot -Force

    # Start Worker
    Write-Output "[...] Starting worker..."
    $workerProcess = Start-Process -FilePath $pythonExe -ArgumentList "bot/worker.py", "-q", "single", "dual" -WorkingDirectory $ProjectRoot `
        -PassThru -NoNewWindow `
        -RedirectStandardOutput $LogFileWorker -RedirectStandardError $LogFileWorkerErr
    $workerProcess.Id | Set-Content $PidFileWorker -Force

    Start-Sleep 5
    Get-ServiceStatus
}

function Get-ServiceStatus {
    Write-Output ""
    Write-Output "=== SERVICE STATUS ==="
    Write-Output ""

    # Redis
    $redisStatus = docker ps --filter "name=$RedisContainer" --format "{{.Status}}" 2>$null
    if ($redisStatus -like "Up*") {
        Write-ColorOutput Green "Redis:  RUNNING ($redisStatus)"
    } else {
        Write-ColorOutput Red "Redis:  STOPPED"
    }

    # Bot (PID file + process existence + our command line)
    $botRunning = $false
    if (Test-Path $PidFileBot) {
        $pidVal = (Get-Content $PidFileBot -Raw -ErrorAction SilentlyContinue).Trim()
        $proc = Get-Process -Id $pidVal -ErrorAction SilentlyContinue
        if ($proc -and (Test-IsOurProcess $pidVal "run.py")) {
            $botRunning = $true
            Write-ColorOutput Green "Bot:    RUNNING (PID $pidVal)"
        } else {
            Remove-Item $PidFileBot -Force -ErrorAction SilentlyContinue
        }
    }
    if (-not $botRunning) {
        Write-ColorOutput Red "Bot:    STOPPED"
    }

    # Worker
    $workerRunning = $false
    if (Test-Path $PidFileWorker) {
        $pidVal = (Get-Content $PidFileWorker -Raw -ErrorAction SilentlyContinue).Trim()
        $proc = Get-Process -Id $pidVal -ErrorAction SilentlyContinue
        if ($proc -and (Test-IsOurProcess $pidVal "worker.py")) {
            $workerRunning = $true
            Write-ColorOutput Green "Worker: RUNNING (PID $pidVal)"
        } else {
            Remove-Item $PidFileWorker -Force -ErrorAction SilentlyContinue
        }
    }
    if (-not $workerRunning) {
        Write-ColorOutput Red "Worker: STOPPED"
    }

    Write-Output ""
}

function Flush-Queue {
    Write-Output "[...] Flushing Redis queues..."
    $result = docker exec $RedisContainer redis-cli -a $RedisPassword FLUSHALL 2>$null
    if ($result -eq "OK") {
        Write-ColorOutput Green "[OK] All queues flushed"
    } else {
        Write-ColorOutput Red "[FAIL] Failed to flush queues (is Redis running?)"
    }
}

function Watch-Logs {
    param([string]$LogFilePath, [string]$Label)

    if (-not (Test-Path $LogFilePath)) {
        Write-ColorOutput Red "[ERROR] No log file for $Label. Run start first."
        return
    }

    Write-Output "=== WATCHING $Label LOGS (Ctrl+C to stop) ==="
    Write-Output ""

    Get-Content -Path $LogFilePath -Wait -Tail 50 | ForEach-Object {
        if ($_ -match "ERROR|FAIL|Exception") {
            Write-ColorOutput Red $_
        } elseif ($_ -match "WARNING") {
            Write-ColorOutput Yellow $_
        } elseif ($_ -match "INFO|Logged in|Listening") {
            Write-ColorOutput Green $_
        } else {
            Write-Output $_
        }
    }
}

function Show-Help {
    Write-Output ""
    Write-Output "Shrimpy Bot Development Utilities"
    Write-Output "================================"
    Write-Output ""
    Write-Output "Usage: .\scripts\dev.ps1 <action>"
    Write-Output ""
    Write-Output "Actions:"
    Write-Output "  start        Start Redis, Bot, and Worker (idempotent: stops existing first)"
    Write-Output "  start-sync   Start with command tree sync (registers /renderbatch etc. with Discord)"
    Write-Output "  stop         Stop Bot and Worker (idempotent; works across sessions)"
    Write-Output "  restart      Stop all, ensure Redis, flush queues, start fresh"
    Write-Output "  status       Show status of all services (PID-based)"
    Write-Output "  logs         Stream bot logs (generated/shrimpy-bot.log)"
    Write-Output "  worker-logs  Stream worker logs (generated/shrimpy-worker.log)"
    Write-Output "  flush        Flush all Redis queues"
    Write-Output "  help         Show this help message"
    Write-Output ""
    Write-Output "Process lifecycle uses PID files in generated/ so start/stop/status work"
    Write-Output "reliably when run via powershell -File or from any terminal."
    Write-Output ""
    Write-Output "Environment Variables:"
    Write-Output "  REDIS_PORT   Redis port (default: 6380)"
    Write-Output ""
    Write-Output "Examples:"
    Write-Output "  .\scripts\dev.ps1 start       # Start everything"
    Write-Output "  .\scripts\dev.ps1 restart     # Clean restart"
    Write-Output "  .\scripts\dev.ps1 logs        # Stream bot logs"
    Write-Output ""
}

# Main
switch ($Action) {
    "start" {
        Start-Services
    }
    "start-sync" {
        $env:SYNC_COMMANDS = "1"
        Start-Services
    }
    "stop" {
        Stop-Services
    }
    "restart" {
        Stop-Services
        Start-Sleep 1
        # Ensure Redis is running before flush (fixes cold-start flush failure)
        if (-not (Start-Redis)) {
            Write-ColorOutput Red "[FAIL] Cannot restart without Redis"
            return
        }
        Start-Sleep 1
        Flush-Queue
        Start-Sleep 1
        Start-Services
    }
    "status" {
        Get-ServiceStatus
    }
    "logs" {
        Watch-Logs -LogFilePath $LogFileBot -Label "ShrimpyBot"
    }
    "worker-logs" {
        Watch-Logs -LogFilePath $LogFileWorker -Label "ShrimpyWorker"
    }
    "flush" {
        Flush-Queue
    }
    "help" {
        Show-Help
    }
}

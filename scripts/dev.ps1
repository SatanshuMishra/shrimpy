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
$PidFileSupervisor = Join-Path $GeneratedDir "shrimpy-supervisor.pid"
$LogFileBot = Join-Path $GeneratedDir "shrimpy-bot.log"
$LogFileBotErr = Join-Path $GeneratedDir "shrimpy-bot.err"
$LogFileSupervisor = Join-Path $GeneratedDir "shrimpy-supervisor.log"
$LogFileSupervisorErr = Join-Path $GeneratedDir "shrimpy-supervisor.err"

# Worker count (1-4, default 1 or from WORKER_COUNT env)
$WorkerCount = if ($env:WORKER_COUNT) { [int]$env:WORKER_COUNT } else { 3 }

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

function Ensure-StoppedSupervisor {
    Stop-ProcessByPidFile $PidFileSupervisor "Supervisor"
    Stop-OrphanProcesses "worker_supervisor.py" "Supervisor"
    # Also kill any orphan workers
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
    Ensure-StoppedSupervisor
}

function Start-Services {
    Ensure-GeneratedDir

    # Idempotent start: ensure no existing process, then start one.
    Ensure-StoppedBot
    Ensure-StoppedSupervisor

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

    # Start Worker Supervisor (manages N workers with auto-respawn)
    Write-Output "[...] Starting worker supervisor ($WorkerCount worker(s))..."
    $supervisorArgs = @("bot/worker_supervisor.py", "-n", $WorkerCount)
    $supervisorProcess = Start-Process -FilePath $pythonExe -ArgumentList $supervisorArgs -WorkingDirectory $ProjectRoot `
        -PassThru -NoNewWindow `
        -RedirectStandardOutput $LogFileSupervisor -RedirectStandardError $LogFileSupervisorErr
    $supervisorProcess.Id | Set-Content $PidFileSupervisor -Force

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

    # Supervisor (manages workers)
    $supervisorRunning = $false
    if (Test-Path $PidFileSupervisor) {
        $pidVal = (Get-Content $PidFileSupervisor -Raw -ErrorAction SilentlyContinue).Trim()
        $proc = Get-Process -Id $pidVal -ErrorAction SilentlyContinue
        if ($proc -and (Test-IsOurProcess $pidVal "worker_supervisor.py")) {
            $supervisorRunning = $true
            Write-ColorOutput Green "Supervisor: RUNNING (PID $pidVal, $WorkerCount worker(s))"
        } else {
            Remove-Item $PidFileSupervisor -Force -ErrorAction SilentlyContinue
        }
    }
    if (-not $supervisorRunning) {
        Write-ColorOutput Red "Supervisor: STOPPED"
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
    Write-Output "  start        Start Redis, Bot, and Worker Supervisor (idempotent: stops existing first)"
    Write-Output "  start-sync   Start with command tree sync (registers /renderbatch etc. with Discord)"
    Write-Output "  stop         Stop Bot and Supervisor (idempotent; works across sessions)"
    Write-Output "  restart      Stop all, ensure Redis, flush queues, start fresh"
    Write-Output "  status       Show status of all services (PID-based)"
    Write-Output "  logs         Stream bot logs (generated/shrimpy-bot.err); set SHIMPY_DEBUG=1 for verbose"
    Write-Output "  worker-logs  Stream supervisor/worker logs (generated/shrimpy-supervisor.err)"
    Write-Output "  flush        Flush all Redis queues"
    Write-Output "  help         Show this help message"
    Write-Output ""
    Write-Output "Process lifecycle uses PID files in generated/ so start/stop/status work"
    Write-Output "reliably when run via powershell -File or from any terminal."
    Write-Output ""
    Write-Output "Environment Variables:"
    Write-Output "  REDIS_PORT    Redis port (default: 6380)"
    Write-Output "  WORKER_COUNT  Number of parallel render workers (1-4, default: 3)"
    Write-Output ""
    Write-Output "The supervisor auto-respawns crashed workers with exponential backoff."
    Write-Output "Failed jobs are retried up to 3 times by RQ before being marked as failed."
    Write-Output ""
    Write-Output "Examples:"
    Write-Output "  .\scripts\dev.ps1 start       # Start everything (3 workers by default)"
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
        # Bot logs go to stderr (StreamHandler); stdout is mostly empty
        Watch-Logs -LogFilePath $LogFileBotErr -Label "ShrimpyBot"
    }
    "worker-logs" {
        Watch-Logs -LogFilePath $LogFileSupervisorErr -Label "ShrimpySupervisor"
    }
    "flush" {
        Flush-Queue
    }
    "help" {
        Show-Help
    }
}

param(
    [switch]$KeepEnvironment,
    [string]$PythonCommand = "python",
    [string]$LogPath = "reports/intermediate-acceptance.log",
    [string]$RuntimeLogPath = "reports/intermediate-runtime.log",
    [string]$EvidencePath = "reports/intermediate-failure-evidence.json"
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ComposeFile = Join-Path $Root "deploy/compose.yaml"
$ResolvedLogPath = Join-Path $Root $LogPath
$ResolvedRuntimeLogPath = Join-Path $Root $RuntimeLogPath
$ResolvedEvidencePath = Join-Path $Root $EvidencePath
$ProjectLabel = "label=com.docker.compose.project=pystream"
$NetworkName = "pystream_pystream"
$OutputVolume = "pystream_pystream-output"
$ImageName = "pystream:0.2.0"
$ProjectVolumeNames = @(
    "pystream_pystream-artifacts",
    "pystream_pystream-checkpoints",
    "pystream_pystream-kafka",
    "pystream_pystream-output",
    "pystream_pystream-worker-1",
    "pystream_pystream-worker-2",
    "pystream_pystream-worker-3"
)
$ToolCounter = 0
$ResourceLedgerPath = Join-Path $Root ".pystream/intermediate-docker-resources.tsv"
$CreatedContainerNames = @()
$CreatedNetworkNames = @()
$CreatedVolumeNames = @()

New-Item -ItemType Directory -Force -Path (Split-Path $ResolvedLogPath) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $ResolvedRuntimeLogPath) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $ResolvedEvidencePath) | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $ResourceLedgerPath) | Out-Null

function ConvertTo-NativeArgument {
    param([AllowEmptyString()][string]$Argument)

    if ($Argument.Length -gt 0 -and $Argument -notmatch '[\s"]') {
        return $Argument
    }
    $Escaped = [Regex]::Replace($Argument, '(\\*)"', '$1$1\"')
    $Escaped = [Regex]::Replace($Escaped, '(\\+)$', '$1$1')
    return '"' + $Escaped + '"'
}

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList,
        [int]$TimeoutSeconds = 600,
        [switch]$Quiet
    )

    if ($TimeoutSeconds -le 0) {
        throw "TimeoutSeconds must be positive"
    }
    $StartInfo = New-Object System.Diagnostics.ProcessStartInfo
    $StartInfo.FileName = $FilePath
    $StartInfo.Arguments = (
        $ArgumentList |
            ForEach-Object { ConvertTo-NativeArgument $_ }
    ) -join " "
    $StartInfo.WorkingDirectory = $Root
    $StartInfo.UseShellExecute = $false
    $StartInfo.CreateNoWindow = $true
    $StartInfo.RedirectStandardOutput = $true
    $StartInfo.RedirectStandardError = $true
    $Process = New-Object System.Diagnostics.Process
    $Process.StartInfo = $StartInfo
    try {
        if (-not $Process.Start()) {
            throw "Unable to start ${FilePath}: $ArgumentList"
        }
        $StdoutTask = $Process.StandardOutput.ReadToEndAsync()
        $StderrTask = $Process.StandardError.ReadToEndAsync()
        $TimedOut = -not $Process.WaitForExit($TimeoutSeconds * 1000)
        if ($TimedOut) {
            $Process.Kill()
        }
        $Process.WaitForExit()
        $OutputLines = @(
            @($StdoutTask.Result -split '\r?\n') +
            @($StderrTask.Result -split '\r?\n') |
                Where-Object { $_.Length -gt 0 }
        )
        if ($TimedOut) {
            throw (
                "$FilePath timed out after ${TimeoutSeconds}s: $ArgumentList`n" +
                ($OutputLines -join "`n")
            )
        }
        if (-not $Quiet) {
            foreach ($Line in $OutputLines) {
                Write-Host $Line
            }
        }
        if ($Process.ExitCode -ne 0) {
            throw (
                "$FilePath exited $($Process.ExitCode): $ArgumentList`n" +
                ($OutputLines -join "`n")
            )
        }
        return $OutputLines
    }
    finally {
        $Process.Dispose()
    }
}

function Invoke-Docker {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    return Invoke-Native "docker" $Arguments -TimeoutSeconds 180
}

function Start-DockerContainers {
    param([Parameter(Mandatory = $true)][string[]]$ContainerNames)

    foreach ($Attempt in 1..3) {
        try {
            Invoke-Native "docker" (@("start") + $ContainerNames) -TimeoutSeconds 60 -Quiet |
                Out-Null
            return
        }
        catch {
            if ($_ -notmatch "timed out") {
                throw
            }

            $Pending = @()
            foreach ($ContainerName in $ContainerNames) {
                try {
                    $StatusLines = @(
                        Invoke-Native "docker" @(
                            "inspect",
                            "--format",
                            "{{.State.Status}}",
                            $ContainerName
                        ) -TimeoutSeconds 15 -Quiet
                    )
                    $Status = ($StatusLines | Select-Object -Last 1).Trim()
                    if ($Status -eq "created") {
                        $Pending += $ContainerName
                    }
                }
                catch {
                    $Pending += $ContainerName
                }
            }
            if ($Pending.Count -eq 0) {
                Write-Host "docker_start_cli_timeout_verified=$($ContainerNames -join ',')"
                return
            }
            if ($Attempt -lt 3) {
                Write-Host (
                    "docker_start_cli_retry=${Attempt};pending=$($Pending -join ',')"
                )
                Start-Sleep -Seconds (2 * $Attempt)
                continue
            }
            throw (
                "docker start timed out after 3 attempts; " +
                "pending=$($Pending -join ',')"
            )
        }
    }
}

function Get-DockerLogs {
    param([Parameter(Mandatory = $true)][string]$ContainerName)

    try {
        return @(
            Invoke-Native "docker" @("logs", $ContainerName) -TimeoutSeconds 30 -Quiet
        )
    }
    catch {
        $Message = "$_"
        if ($Message -notmatch "timed out") {
            throw
        }
        $Lines = @($Message -split '\r?\n' | Select-Object -Skip 1)
        if ($Lines.Count -eq 0) {
            throw
        }
        return $Lines
    }
}

function Invoke-DockerCreateResource {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$ResourceType,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $SuccessPattern = if ($ResourceType -eq "volume") {
        "(?m)^$([Regex]::Escape($Name))$"
    }
    else {
        "(?m)^[0-9a-f]{64}$"
    }
    foreach ($Attempt in 1..3) {
        try {
            Invoke-Native "docker" $Arguments -TimeoutSeconds 60 | Out-Null
            return
        }
        catch {
            $Message = "$_"
            if ($Message -match $SuccessPattern) {
                return
            }
            if ($Message -match "timed out" -and $Attempt -lt 3) {
                Start-Sleep -Seconds (2 * $Attempt)
                continue
            }
            throw
        }
    }
}

function Get-ServiceContainerId {
    param([Parameter(Mandatory = $true)][string]$Service)

    return "pystream-${Service}-1"
}

function Wait-ContainerHealth {
    param(
        [Parameter(Mandatory = $true)][string]$Service,
        [int]$TimeoutSeconds = 120
    )

    $ContainerId = Get-ServiceContainerId $Service
    $Deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $LastStatus = "unknown"
    while ([DateTime]::UtcNow -lt $Deadline) {
        try {
            $StatusLines = @(
                Invoke-Native "docker" @(
                    "inspect",
                    "--format",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                    $ContainerId
                ) -TimeoutSeconds 30 -Quiet
            )
        }
        catch {
            if ($_ -match "timed out") {
                $LastStatus = "inspect_timeout"
                continue
            }
            throw
        }
        $LastStatus = ($StatusLines | Select-Object -Last 1).Trim()
        if ($LastStatus -eq "healthy") {
            Write-Host "service_healthy=$Service"
            return
        }
        if ($LastStatus -in @("dead", "exited")) {
            Invoke-Docker logs $ContainerId
            throw "Service $Service entered $LastStatus before becoming healthy"
        }
        Start-Sleep -Seconds 1
    }
    Invoke-Docker logs $ContainerId
    throw "Service $Service health timeout; last_status=$LastStatus"
}

function Wait-ContainerExit {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [int]$TimeoutSeconds = 180
    )

    $Deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $LastStatus = "unknown"
    while ([DateTime]::UtcNow -lt $Deadline) {
        try {
            $StateLines = @(
                Invoke-Native "docker" @(
                    "inspect",
                    "--format",
                    "{{.State.Status}} {{.State.ExitCode}}",
                    $ContainerName
                ) -TimeoutSeconds 15 -Quiet
            )
        }
        catch {
            if ($_ -match "timed out") {
                $LastStatus = "inspect_timeout"
                continue
            }
            throw
        }
        $State = ($StateLines | Select-Object -Last 1).Trim()
        $Parts = @($State -split " ", 2)
        if ($Parts.Count -ne 2) {
            throw "Container $ContainerName returned invalid state: $State"
        }
        $LastStatus = $Parts[0]
        if ($LastStatus -eq "exited") {
            return [int]$Parts[1]
        }
        if ($LastStatus -eq "dead") {
            throw "Container $ContainerName entered dead state"
        }
        Start-Sleep -Milliseconds 250
    }
    throw "Container $ContainerName exit timeout; last_status=$LastStatus"
}

function Remove-KnownDockerResources {
    param(
        [Parameter(Mandatory = $true)][string]$ResourceType,
        [Parameter(Mandatory = $true)][string[]]$Names
    )

    $Arguments = @($ResourceType, "rm", "-f") + $Names
    foreach ($Attempt in 1..3) {
        try {
            Invoke-Native "docker" $Arguments -TimeoutSeconds 60 -Quiet | Out-Null
            return
        }
        catch {
            $Message = "$_"
            $Confirmed = @(
                $Names |
                    Where-Object {
                        $EscapedName = [Regex]::Escape($_)
                        $Message -match "(?m)^${EscapedName}$" -or
                        $Message -match "(No such|not found).*$EscapedName"
                    }
            )
            if ($Confirmed.Count -eq $Names.Count) {
                return
            }
            $DaemonErrors = @(
                $Message -split '\r?\n' |
                    Where-Object { $_ -match "Error response from daemon:" }
            )
            $Unexpected = @(
                $DaemonErrors |
                    Where-Object {
                        $_ -notmatch "No such (container|network|volume)" -and
                        $_ -notmatch "not found"
                    }
            )
            if ($DaemonErrors.Count -gt 0 -and $Unexpected.Count -eq 0) {
                return
            }
            if ($Message -match "timed out" -and $Attempt -lt 3) {
                Start-Sleep -Seconds (2 * $Attempt)
                continue
            }
            throw
        }
    }
}

function Register-DockerResource {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("container", "network", "volume")]
        [string]$ResourceType,
        [Parameter(Mandatory = $true)][string]$Name
    )

    switch ($ResourceType) {
        "container" { $script:CreatedContainerNames += $Name }
        "network" { $script:CreatedNetworkNames += $Name }
        "volume" { $script:CreatedVolumeNames += $Name }
    }
    $Encoding = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::AppendAllText(
        $ResourceLedgerPath,
        "${ResourceType}`t${Name}`n",
        $Encoding
    )
}

function Remove-ComposeProject {
    $Containers = @($script:CreatedContainerNames)
    $Networks = @($script:CreatedNetworkNames)
    $Volumes = @($script:CreatedVolumeNames)
    if (Test-Path $ResourceLedgerPath) {
        foreach ($Line in Get-Content -Path $ResourceLedgerPath -Encoding utf8) {
            $Parts = @($Line -split "`t", 2)
            if ($Parts.Count -ne 2) {
                throw "Invalid Docker resource ledger line: $Line"
            }
            switch ($Parts[0]) {
                "container" { $Containers += $Parts[1] }
                "network" { $Networks += $Parts[1] }
                "volume" { $Volumes += $Parts[1] }
                default { throw "Invalid Docker resource type in ledger: $($Parts[0])" }
            }
        }
    }
    $Containers = @($Containers | Sort-Object -Unique)
    $Networks = @($Networks | Sort-Object -Unique)
    $Volumes = @($Volumes | Sort-Object -Unique)
    if ($Containers.Count -gt 0) {
        Remove-KnownDockerResources "container" $Containers
    }
    if ($Networks.Count -gt 0) {
        Remove-KnownDockerResources "network" $Networks
    }
    if ($Volumes.Count -gt 0) {
        Remove-KnownDockerResources "volume" $Volumes
    }
    Remove-Item -Force -ErrorAction SilentlyContinue $ResourceLedgerPath
    $script:CreatedContainerNames = @()
    $script:CreatedNetworkNames = @()
    $script:CreatedVolumeNames = @()
}

function Assert-ComposeProjectRemoved {
    if (Test-Path $ResourceLedgerPath) {
        throw "Docker resource ledger remains after cleanup: $ResourceLedgerPath"
    }
    Write-Host "compose_project_resources=0"
}

function New-PyStreamContainerArguments {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][string]$Hostname
    )

    return @(
        "create",
        "--name", $Name,
        "--hostname", $Hostname,
        "--label", "com.docker.compose.project=pystream",
        "--label", "com.docker.compose.service=$Service",
        "--user", "10001:10001",
        "--init",
        "--read-only",
        "--tmpfs", "/tmp:size=64m,mode=1777",
        "--restart", "unless-stopped",
        "--stop-timeout", "20",
        "--security-opt", "no-new-privileges:true",
        "--cap-drop", "ALL",
        "--network", $NetworkName
    )
}

function Initialize-DockerProject {
    Invoke-DockerCreateResource @(
        "network", "create",
        "--label", "com.docker.compose.project=pystream",
        "--driver", "bridge",
        $NetworkName
    ) "network" $NetworkName
    Register-DockerResource "network" $NetworkName
    foreach ($Name in $ProjectVolumeNames) {
        Invoke-DockerCreateResource @(
            "volume", "create",
            "--label", "com.docker.compose.project=pystream",
            $Name
        ) "volume" $Name
        Register-DockerResource "volume" $Name
    }

    $KafkaImage = (
        "apache/kafka:3.9.1@" +
        "sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed"
    )
    $KafkaHealth = (
        "/opt/kafka/bin/kafka-topics.sh " +
        "--bootstrap-server localhost:9092 --list >/dev/null 2>&1"
    )
    $KafkaArguments = @(
        "create",
        "--name", "pystream-kafka-1",
        "--hostname", "kafka",
        "--label", "com.docker.compose.project=pystream",
        "--label", "com.docker.compose.service=kafka",
        "--restart", "unless-stopped",
        "--stop-timeout", "30",
        "--network", $NetworkName,
        "--health-cmd", $KafkaHealth,
        "--health-interval", "5s",
        "--health-timeout", "5s",
        "--health-retries", "20",
        "--health-start-period", "20s",
        "--cpus", "1.00",
        "--memory", "1g",
        "-e", "KAFKA_NODE_ID=1",
        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
        "-e", "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        "-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
        "-e", "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
        "-e", "KAFKA_LOG_DIRS=/var/lib/kafka/data",
        "-v", "pystream_pystream-kafka:/var/lib/kafka/data",
        $KafkaImage
    )
    Invoke-DockerCreateResource $KafkaArguments "container" "pystream-kafka-1"
    Register-DockerResource "container" "pystream-kafka-1"

    $InitCommand = (
        "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 " +
        "--create --if-not-exists --topic words --partitions 2 --replication-factor 1 " +
        "&& /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 " +
        "--create --if-not-exists --topic intermediate-words --partitions 2 " +
        "--replication-factor 1"
    )
    $KafkaInitArguments = @(
        "create",
        "--name", "pystream-kafka-init-1",
        "--label", "com.docker.compose.project=pystream",
        "--label", "com.docker.compose.service=kafka-init",
        "--network", $NetworkName,
        "--cpus", "0.25",
        "--memory", "256m",
        $KafkaImage,
        "/bin/bash", "-ec", $InitCommand
    )
    Invoke-DockerCreateResource $KafkaInitArguments "container" "pystream-kafka-init-1"
    Register-DockerResource "container" "pystream-kafka-init-1"

    $JobManagerHealth = (
        "python -c " +
        "`"import json,urllib.request; " +
        "d=json.load(urllib.request.urlopen('http://localhost:8080/health', timeout=2)); " +
        "assert d['status']=='ok'`""
    )
    $JobManagerArguments = @(
        New-PyStreamContainerArguments `
            "pystream-jobmanager-1" `
            "jobmanager" `
            "jobmanager"
    ) + @(
        "-p", "8080:8080",
        "-v", "pystream_pystream-artifacts:/data/artifacts",
        "-v", "pystream_pystream-checkpoints:/data/checkpoints",
        "--health-cmd", $JobManagerHealth,
        "--health-interval", "5s",
        "--health-timeout", "3s",
        "--health-retries", "12",
        "--health-start-period", "5s",
        "--cpus", "0.50",
        "--memory", "512m",
        $ImageName,
        "python", "-m", "pystream.service",
        "jobmanager",
        "--host", "0.0.0.0",
        "--port", "8080",
        "--artifact-root", "/data/artifacts"
    )
    Invoke-DockerCreateResource $JobManagerArguments "container" "pystream-jobmanager-1"
    Register-DockerResource "container" "pystream-jobmanager-1"

    $WorkerHealth = (
        "python -c " +
        "`"import json,urllib.request; " +
        "d=json.load(urllib.request.urlopen('http://localhost:8081/health', timeout=2)); " +
        "assert d['status']=='ok'`""
    )
    foreach ($Index in 1..3) {
        $Service = "worker-$Index"
        $ContainerName = "pystream-${Service}-1"
        $WorkerArguments = @(
            New-PyStreamContainerArguments `
                $ContainerName `
                $Service `
                $Service
        ) + @(
            "-v", "pystream_pystream-checkpoints:/data/checkpoints",
            "-v", "pystream_pystream-output:/data/output",
            "-v", "pystream_pystream-${Service}:/data/work",
            "--health-cmd", $WorkerHealth,
            "--health-interval", "5s",
            "--health-timeout", "3s",
            "--health-retries", "12",
            "--health-start-period", "10s",
            "--cpus", "0.50",
            "--memory", "512m",
            $ImageName,
            "python", "-m", "pystream.service",
            "worker",
            "--worker-id", $Service,
            "--control-address", "http://${Service}:8081",
            "--data-host", $Service,
            "--data-port", "9000",
            "--slots", "4",
            "--jobmanager-url", "http://jobmanager:8080",
            "--work-root", "/data/work"
        )
        Invoke-DockerCreateResource $WorkerArguments "container" $ContainerName
        Register-DockerResource "container" $ContainerName
    }
}

function Start-ComposeProject {
    Initialize-DockerProject
    Start-Sleep -Seconds 2

    $KafkaId = Get-ServiceContainerId "kafka"
    $JobManagerId = Get-ServiceContainerId "jobmanager"
    Start-DockerContainers @($KafkaId, $JobManagerId)
    Wait-ContainerHealth "kafka"
    Wait-ContainerHealth "jobmanager"

    $KafkaInitId = Get-ServiceContainerId "kafka-init"
    Start-DockerContainers @($KafkaInitId)
    $KafkaInitExit = Wait-ContainerExit $KafkaInitId 120
    Get-DockerLogs $KafkaInitId
    if ($KafkaInitExit -ne 0) {
        throw "kafka-init exited $KafkaInitExit"
    }

    $WorkerIds = @(
        Get-ServiceContainerId "worker-1"
        Get-ServiceContainerId "worker-2"
        Get-ServiceContainerId "worker-3"
    )
    Start-DockerContainers $WorkerIds
    Wait-ContainerHealth "worker-1"
    Wait-ContainerHealth "worker-2"
    Wait-ContainerHealth "worker-3"
}

function Invoke-Tools {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)

    $script:ToolCounter += 1
    $ContainerName = "pystream-tool-$script:ToolCounter"
    $CreateArguments = @(
        "create",
        "--name", $ContainerName,
        "--label", "com.docker.compose.project=pystream",
        "--label", "com.docker.compose.service=tools-run",
        "--network", $NetworkName,
        "--user", "10001:10001",
        "--read-only",
        "--tmpfs", "/tmp:size=64m,mode=1777",
        "--security-opt", "no-new-privileges:true",
        "--cap-drop", "ALL",
        "-e", "PYSTREAM_JOBMANAGER_URL=http://jobmanager:8080",
        "-e", "PYSTREAM_KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
        "-e", "PYSTREAM_OUTPUT_ROOT=/data/output",
        "-v", "${OutputVolume}:/data/output",
        $ImageName,
        "python"
    ) + $Arguments
    Invoke-DockerCreateResource $CreateArguments "container" $ContainerName
    Register-DockerResource "container" $ContainerName
    Start-DockerContainers @($ContainerName)
    $ExitCode = Wait-ContainerExit $ContainerName
    $OutputLines = @(Get-DockerLogs $ContainerName)
    if ($ExitCode -ne 0) {
        foreach ($Line in $OutputLines) {
            Write-Host $Line
        }
        throw "Tool container $ContainerName exited $ExitCode"
    }
    return $OutputLines
}

function Save-RuntimeLogs {
    $Lines = @()
    foreach ($Service in @("jobmanager", "worker-1", "worker-2", "worker-3")) {
        $ContainerId = Get-ServiceContainerId $Service
        $Lines += "===== $Service $ContainerId ====="
        try {
            $Lines += @(Get-DockerLogs $ContainerId)
        }
        catch {
            if ($_ -notmatch "No such container") {
                throw
            }
            $Lines += "container_not_found"
        }
    }
    $Lines | Out-File -FilePath $ResolvedRuntimeLogPath -Encoding utf8
}

function Get-JobId {
    param([object[]]$Lines)

    $match = $Lines | Select-String -Pattern "^job_id=([0-9a-f]+)$" | Select-Object -Last 1
    if ($null -eq $match) {
        throw "submit output did not contain job_id"
    }
    return $match.Matches[0].Groups[1].Value
}

Set-Location $Root
Start-Transcript -Path $ResolvedLogPath -Force | Out-Null
$IntermediateJobId = $null
try {
    Remove-ComposeProject
    Assert-ComposeProjectRemoved
    Start-ComposeProject

    Write-Host "== Basic WordCount compatibility =="
    Invoke-Tools "/opt/pystream/scripts/produce_wordcount.py"
    $WordCountSubmission = @(
        Invoke-Tools "/opt/pystream/scripts/submit_wordcount.py"
    )
    $WordCountJobId = Get-JobId $WordCountSubmission
    Invoke-Tools "/opt/pystream/scripts/wait_for_window.py" "--job-id" $WordCountJobId
    Invoke-Tools "/opt/pystream/scripts/verify_wordcount.py" "--job-id" $WordCountJobId
    Invoke-Tools "/opt/pystream/scripts/cleanup_wordcount.py" "--job-id" $WordCountJobId

    Write-Host "== Intermediate baseline and checkpoint =="
    Invoke-Tools "/opt/pystream/scripts/produce_intermediate.py" "--phase" "baseline"
    $IntermediateSubmission = @(
        Invoke-Tools "/opt/pystream/scripts/submit_intermediate.py" `
            "--checkpoint-interval" "1h"
    )
    $IntermediateJobId = Get-JobId $IntermediateSubmission
    Invoke-Tools "/opt/pystream/scripts/wait_for_intermediate.py" `
        "--job-id" $IntermediateJobId "--phase" "baseline"
    Invoke-Tools "/opt/pystream/scripts/wait_for_checkpoint.py" `
        "--job-id" $IntermediateJobId "--min-checkpoint" "1" "--trigger"

    Write-Host "== Post-checkpoint records and fault injection =="
    Invoke-Tools "/opt/pystream/scripts/produce_intermediate.py" "--phase" "recovery"
    Invoke-Tools "/opt/pystream/scripts/wait_for_intermediate.py" `
        "--job-id" $IntermediateJobId "--phase" "recovery"

    Invoke-Native $PythonCommand @(
        "scripts/inject_worker_failure.py",
        "--job-id", $IntermediateJobId,
        "--compose-file", $ComposeFile,
        "--evidence-path", $ResolvedEvidencePath
    )

    Write-Host "== Post-recovery checkpoint and At-least-once verification =="
    Invoke-Tools "/opt/pystream/scripts/wait_for_checkpoint.py" `
        "--job-id" $IntermediateJobId `
        "--min-attempt" "1" `
        "--require-post-recovery" `
        "--trigger"
    Invoke-Tools "/opt/pystream/scripts/verify_intermediate.py" `
        "--job-id" $IntermediateJobId `
        "--min-attempt" "1"
    Invoke-Tools "/opt/pystream/scripts/cleanup_intermediate.py" `
        "--job-id" $IntermediateJobId `
        "--keep-output"

    Write-Host "intermediate_acceptance=passed"
    Write-Host "job_id=$IntermediateJobId"
}
finally {
    $CleanupFailure = $null
    try {
        Save-RuntimeLogs
    }
    catch {
        Write-Warning "runtime log collection failed: $_"
    }
    if (-not $KeepEnvironment) {
        try {
            Remove-ComposeProject
            Assert-ComposeProjectRemoved
        }
        catch {
            $CleanupFailure = $_
        }
    }
    Stop-Transcript | Out-Null
    if ($null -ne $CleanupFailure) {
        throw "project cleanup failed: $CleanupFailure"
    }
}

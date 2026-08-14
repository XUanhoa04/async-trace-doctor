param([string[]]$Scenarios = @('normal','no_inject','no_extract','batch_incomplete','duplicate'))
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$results = @()
try {
  docker compose config --quiet
  docker compose build
  foreach ($scenario in $Scenarios) {
    [string[]]$expected = @(switch ($scenario) {
      'normal' { @() }
      'no_inject' { @('ATD-CTX-001') }
      'no_extract' { @('ATD-CTX-001') }
      'batch_incomplete' { @('ATD-BAT-001') }
      'duplicate' { @('ATD-DUP-001') }
    })
    docker compose down --volumes --remove-orphans | Out-Null
    $env:ATD_FAULT_MODE = $scenario
    # Start the long-lived infrastructure first, then the consumer. Starting
    # producer and consumer in the same compose transaction is racy on fresh
    # brokers: a fast producer can finish before the consumer has joined its
    # group, which occasionally leaves the one-shot consumer at its timeout.
    docker compose up -d redpanda async-trace-doctor otel-collector prometheus
    docker compose up -d consumer
    Start-Sleep -Seconds 2
    docker compose up -d producer

    # Compose returns a workload container's exit code. Capture it explicitly
    # so diagnostics survive instead of letting PowerShell jump to finally and
    # remove every container before logs are printed.
    $PSNativeCommandUseErrorActionPreference = $false
    docker compose wait producer consumer | Out-Null
    $waitExit = $LASTEXITCODE
    $PSNativeCommandUseErrorActionPreference = $true
    if ($waitExit -ne 0) {
      Write-Error "Scenario '$scenario' workload exited with code $waitExit" -ErrorAction Continue
      docker compose ps -a
      docker compose logs --no-color producer consumer redpanda otel-collector async-trace-doctor
      $results += [ordered]@{ scenario=$scenario; passed=$false; expected_rule_ids=$expected; observed_rule_ids=@(); audited_spans=0; findings=0; error="workload_exit_$waitExit" }
      break
    }

    $report = $null
    $requiredSpans = switch ($scenario) {
      'batch_incomplete' { 5 }
      'duplicate' { 16 }
      default { 8 }
    }
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
      Start-Sleep -Seconds 1
      $report = Invoke-RestMethod -Uri 'http://localhost:18080/report' -TimeoutSec 10
      if ($report.summary.audited_spans -ge $requiredSpans) { break }
    }
    $ids = @($report.findings | ForEach-Object { $_.rule_id } | Sort-Object -Unique)
    $missing = @($expected | Where-Object { $_ -notin $ids })
    $passed = ($report.summary.audited_spans -ge $requiredSpans) -and ($missing.Count -eq 0) -and ($scenario -ne 'normal' -or $ids.Count -eq 0)
    $results += [ordered]@{ scenario=$scenario; passed=$passed; expected_rule_ids=$expected; observed_rule_ids=$ids; audited_spans=$report.summary.audited_spans; findings=$report.summary.violations }
  }
} finally {
  Remove-Item Env:ATD_FAULT_MODE -ErrorAction SilentlyContinue
  docker compose down --volumes --remove-orphans | Out-Null
}
$artifact = [ordered]@{
  status = if (($results | Where-Object { -not $_.passed }).Count -eq 0) {'passed'} else {'failed'}
  executed_at = (Get-Date).ToUniversalTime().ToString('o')
  git_commit = (git rev-parse HEAD).Trim()
  git_worktree_dirty = [bool]((git status --porcelain) -join '')
  rules_sha256 = (Get-FileHash -Algorithm SHA256 'config/rules.yaml').Hash.ToLowerInvariant()
  scenarios = $results
}
New-Item -ItemType Directory -Force -Path 'evaluation/results' | Out-Null
$artifact | ConvertTo-Json -Depth 8 | Set-Content -Encoding utf8 'evaluation/results/live.json'
if ($artifact.status -ne 'passed') { throw 'One or more live E2E scenarios failed. See evaluation/results/live.json.' }

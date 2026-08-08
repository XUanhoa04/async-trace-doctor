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
    docker compose down --volumes --remove-orphans | Out-Null
    $env:ATD_FAULT_MODE = $scenario
    docker compose up -d
    docker compose wait producer consumer | Out-Null
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
	[string[]]$expected = @(switch ($scenario) {
      'normal' { @() }
      'no_inject' { @('ATD-CTX-001') }
      'no_extract' { @('ATD-CTX-001') }
      'batch_incomplete' { @('ATD-BAT-001') }
      'duplicate' { @('ATD-DUP-001') }
	})
    $missing = @($expected | Where-Object { $_ -notin $ids })
	$passed = ($report.summary.audited_spans -ge $requiredSpans) -and ($missing.Count -eq 0) -and ($scenario -ne 'normal' -or $ids.Count -eq 0)
    $results += [ordered]@{ scenario=$scenario; passed=$passed; expected_rule_ids=$expected; observed_rule_ids=$ids; audited_spans=$report.summary.audited_spans; findings=$report.summary.violations }
  }
} finally {
  Remove-Item Env:ATD_FAULT_MODE -ErrorAction SilentlyContinue
  docker compose down --volumes --remove-orphans | Out-Null
}
$artifact = [ordered]@{ status = if (($results | Where-Object { -not $_.passed }).Count -eq 0) {'passed'} else {'failed'}; executed_at=(Get-Date).ToUniversalTime().ToString('o'); scenarios=$results }
New-Item -ItemType Directory -Force -Path 'evaluation/results' | Out-Null
$artifact | ConvertTo-Json -Depth 8 | Set-Content -Encoding utf8 'evaluation/results/live.json'
if ($artifact.status -ne 'passed') { throw 'One or more live E2E scenarios failed. See evaluation/results/live.json.' }

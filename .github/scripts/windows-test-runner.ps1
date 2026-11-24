$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

function Report-TestCrash([string]$stage, [string]$detail) {
    $escaped = $detail.Replace('%', '%25').Replace("`r", '%0D').Replace("`n", '%0A')
    Write-Host "::error title=${stage}::$escaped"
}

foreach ($mode in 'default', 'fdk-aac-decoder') {
    $features = if ($mode -ne 'default') { '--features', $mode } else { @() }
    $artifacts = cargo test --lib --locked @features --no-run --message-format=json
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    $test = ($artifacts | ConvertFrom-Json | Where-Object {
        $_.reason -eq 'compiler-artifact' -and $_.profile.test -and $_.executable -and $_.target.name -eq 'light_ear'
    }).executable
    if (!$test) { throw "Could not locate the $mode library test executable" }
    & $test
    $code = $LASTEXITCODE
    if ($code -eq 0) { continue }
    Report-TestCrash "Windows $mode" "Test executable exited with $code"
    if ($code -in -1073741819, 3221225477, 139) {
        $debugger = Get-Command lldb.exe -ErrorAction SilentlyContinue
        if (!$debugger -and (Test-Path "$env:ProgramFiles\LLVM\bin\lldb.exe")) {
            $debugger = Get-Command "$env:ProgramFiles\LLVM\bin\lldb.exe"
        }
        if ($debugger) {
            $trace = & $debugger.Source --batch -o run -k 'thread backtrace -c 24' -- $test --nocapture 2>&1
            $trace | Select-Object -Last 60 | Write-Host
            Report-TestCrash "Windows debugger" (($trace | Select-Object -Last 26) -join "`n")
        }
        $serial = & $test --test-threads=1 --nocapture 2>&1
        $serialCode = $LASTEXITCODE
        Report-TestCrash "Serial diagnostic" ("Exit $serialCode`n" + (($serial | Select-Object -Last 15) -join "`n"))
    }
    exit 1
}

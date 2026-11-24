# Cargo passes the compiled test executable and its libtest arguments here.
param([string]$TestBinary, [Parameter(ValueFromRemainingArguments)] [string[]]$TestArgs)

& $TestBinary @TestArgs
$testExit = $LASTEXITCODE
if ($testExit -eq -1073741819) {
    # A native access violation kills libtest before it can report a failing test.
    # Re-run the same binary under the runner's debugger to capture the faulting stack.
    $debugger = Get-Command lldb.exe -ErrorAction SilentlyContinue
    if (!$debugger -and (Test-Path "$env:ProgramFiles\LLVM\bin\lldb.exe")) {
        $debugger = Get-Command "$env:ProgramFiles\LLVM\bin\lldb.exe"
    }
    if ($debugger) {
        $trace = & $debugger.Source --batch -o run -k 'thread backtrace -c 24' -- $TestBinary @TestArgs --nocapture 2>&1
        $trace | Select-Object -Last 60 | Write-Host
        $frames = ($trace | Select-String 'stop reason|frame #\d+:' | Select-Object -Last 25) -join "`n"
        if ($frames) {
            $escaped = $frames.Replace('%', '%25').Replace("`r", '%0D').Replace("`n", '%0A')
            Write-Host "::error title=Windows test crash::$escaped"
        }
    } else {
        $serial = & $TestBinary @TestArgs --test-threads=1 --nocapture 2>&1
        $serial | Select-Object -Last 30 | Write-Host
        $lastTest = $serial | Select-String '^test ' | Select-Object -Last 1
        Write-Host "::error title=Windows test crash::Serial diagnostic last reported $lastTest (exit $LASTEXITCODE)"
    }
}
exit $testExit

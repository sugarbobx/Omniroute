Set-Location "C:\Omniroute"

$python  = "C:\Omniroute\venv\Scripts\python.exe"
$logFile = "C:\Omniroute\service.log"

function Write-Log($msg) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    "$ts  $msg" | Out-File -Append -Encoding utf8 $logFile
}

Write-Log "OmniRoute service wrapper started."

while ($true) {
    Write-Log "Starting uvicorn..."
    $proc = Start-Process -FilePath $python `
        -ArgumentList "-m","uvicorn","main:app","--host","0.0.0.0","--port","8000" `
        -WorkingDirectory "C:\Omniroute" `
        -PassThru -NoNewWindow

    Write-Log "PID $($proc.Id)"
    $proc.WaitForExit()
    $code = $proc.ExitCode
    Write-Log "Process exited with code $code. Restarting in 5 s..."
    Start-Sleep -Seconds 5
}

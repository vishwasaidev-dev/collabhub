# CollabHub launcher -- idempotent (safe to run even if already up), used both
# manually and by the "CollabHub Autostart" Scheduled Task (At log on).
$ErrorActionPreference = "SilentlyContinue"
$dir  = "C:\Users\vkarh\OneDrive\Documents\claude Projects\collabhub"
$py   = Join-Path $dir ".venv\Scripts\python.exe"
$log  = Join-Path $dir "collabhub.log"
$errl = Join-Path $dir "collabhub.err.log"

$listening = Get-NetTCPConnection -LocalPort 8765 -State Listen -ErrorAction SilentlyContinue
if ($listening) {
    Write-Output "CollabHub already listening on 8765, not starting a second copy."
} else {
    Start-Process -FilePath $py -ArgumentList "app.py" -WorkingDirectory $dir `
        -WindowStyle Hidden -RedirectStandardOutput $log -RedirectStandardError $errl
    Write-Output "Started CollabHub (logs: $log / $errl)"
}

# 停掉本專案在背景執行的 Python 程式（simulator / dispatcher / uvicorn）
# 只會處理命令列中含有本專案資料夾路徑的 python 行程。
$root = (Resolve-Path "$PSScriptRoot\..").Path
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
    Where-Object { $_.CommandLine -like "*$root*" -or $_.ExecutablePath -like "$root*" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue; "stopped $($_.ProcessId)" }

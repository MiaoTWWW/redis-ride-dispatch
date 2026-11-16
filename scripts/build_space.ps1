# Assemble the files a Hugging Face Space needs into .\hf-space\
# (code + Dockerfile + Space README only; no docs, images, reports or dumps).
#   powershell -ExecutionPolicy Bypass -File scripts\build_space.ps1
$ErrorActionPreference = 'Stop'
$root = (Resolve-Path "$PSScriptRoot\..").Path
$out = Join-Path $root 'hf-space'

if (Test-Path $out) {
    Get-ChildItem $out -Force | Where-Object { $_.Name -ne '.git' } | Remove-Item -Recurse -Force
}
New-Item -ItemType Directory -Force $out, "$out\data", "$out\deploy" | Out-Null

foreach ($f in 'Dockerfile', 'requirements.txt', 'config.py', 'rides.py', 'simulator.py', 'dispatcher.py', 'app.py') {
    Copy-Item (Join-Path $root $f) $out
}
Copy-Item (Join-Path $root 'lua') $out -Recurse
Copy-Item (Join-Path $root 'static') $out -Recurse
Copy-Item (Join-Path $root 'data\mrt_stations.csv') "$out\data"
Copy-Item (Join-Path $root 'deploy\start.sh') "$out\deploy"
Copy-Item (Join-Path $root 'deploy\space_README.md') "$out\README.md"

Write-Host "Space files written to $out"
Get-ChildItem $out -Recurse -File | ForEach-Object { $_.FullName.Substring($out.Length + 1) }

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::UTF8

python -m py_compile goliath/main.py

Write-Host "OK: goliath/main.py syntax is valid"

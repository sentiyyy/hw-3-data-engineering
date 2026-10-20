$ErrorActionPreference = "Stop"
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Создан .env. Проверьте локальные ключи перед запуском."
}
docker compose up -d rustfs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
docker compose run --rm pipeline all --limit 1200 --chunk-sizes 100,500,1000 --attempts 3
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "Готово. Откройте artifacts/report.pdf и проверьте объекты в http://localhost:9001"

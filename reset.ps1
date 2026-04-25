# reset.ps1
# Run this whenever Kafka crashes with cluster ID mismatch.
# Wipes Kafka + Zookeeper local data and restarts everything cleanly.

Write-Host "Stopping all containers..." -ForegroundColor Yellow
docker compose down -v

Write-Host "Wiping Kafka and Zookeeper data..." -ForegroundColor Yellow
if (Test-Path "docker-data") {
    Remove-Item -Recurse -Force "docker-data"
}

Write-Host "Initialising Airflow..." -ForegroundColor Yellow
docker compose up airflow-init

Write-Host "Starting all services..." -ForegroundColor Yellow
docker compose up -d

Write-Host ""
Write-Host "Done! Services available at:" -ForegroundColor Green
Write-Host "  Airflow    -> http://localhost:8080  (admin / admin)"
Write-Host "  Streamlit  -> http://localhost:8501"
Write-Host "  Kafka UI   -> http://localhost:8081"
docker compose down
docker compose up --build -d
Start-Sleep -Seconds 3
Start-Process "http://localhost:8501"
docker stop redis_snapper
Start-Sleep 5
docker rm redis_snapper
Start-Sleep 5
docker compose up -d
Start-Sleep 5

Write-Output "Redis is started"
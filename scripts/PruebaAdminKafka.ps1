# Requiere API local iniciada y variables VETCARE_ADMIN_* en esta terminal.
# Prueba controlada: nunca elimina los topicos criticos de VetCare.
$ErrorActionPreference = 'Stop'
if (-not $env:VETCARE_ADMIN_USERNAME -or -not $env:VETCARE_ADMIN_PASSWORD) {
    throw 'Faltan VETCARE_ADMIN_USERNAME y VETCARE_ADMIN_PASSWORD en esta terminal.'
}
$base = 'http://127.0.0.1:8086/api/admin/kafka'
$raw = "$($env:VETCARE_ADMIN_USERNAME):$($env:VETCARE_ADMIN_PASSWORD)"
$token = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($raw))
$headers = @{ Authorization = "Basic $token" }
$demo = 'vetcare.demo.' + [guid]::NewGuid().ToString('N').Substring(0,8)
$created = $false
try {
    Write-Host "=== CLUSTER ==="
    Invoke-RestMethod -Uri "$base/cluster" -Headers $headers | Format-List
    Write-Host "=== CREAR TOPICO DE PRUEBA: $demo ==="
    $body = @{ name = $demo; partitions = 3; replicationFactor = 3;
        configurations = @{ 'retention.ms' = '86400000'; 'cleanup.policy' = 'delete' } } | ConvertTo-Json -Depth 6
    Invoke-RestMethod -Uri "$base/topics" -Method Post -Headers $headers `
        -ContentType 'application/json' -Body $body | Format-List
    $created = $true
    Write-Host "=== ACTUALIZAR RETENCION ==="
    $body = @{ configurations = @{ 'retention.ms' = '172800000' } } | ConvertTo-Json -Depth 6
    Invoke-RestMethod -Uri "$base/topics/$demo" -Method Patch -Headers $headers `
        -ContentType 'application/json' -Body $body | Format-List
    Write-Host "=== GRUPOS DE CONSUMIDORES ==="
    Invoke-RestMethod -Uri "$base/consumer-groups" -Headers $headers | Format-Table
    Write-Host "=== PRUEBA REST COMPLETADA ==="
}
finally {
    if ($created) {
        Write-Host "=== ELIMINANDO TOPICO DE DEMOSTRACION: $demo ==="
        Invoke-RestMethod -Uri "$base/topics/$demo" -Method Delete -Headers $headers
    }
}

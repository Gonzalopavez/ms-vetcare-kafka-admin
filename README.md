# VetCare — Microservicio Administrador Kafka (EV3)

Microservicio Spring Boot **4.1.1 / Java 17** para administrar un clúster Apache Kafka mediante una API REST. Sin PostgreSQL, JPA ni RabbitMQ.

## Antes de comenzar

Requiere un clúster accesible desde la máquina que ejecute el servicio y un JDK 17+.

Este ZIP es **código fuente preparado para compilar y probar en Windows**. No afirma que haya sido ejecutado en el equipo del estudiante; hay que correr Maven y una prueba REST local.

### Preparar el Maven Wrapper

Como la solución se integra en la carpeta `vetcare-ev3` junto a `ms-Audit`, reutiliza el *wrapper* Maven (herramienta de desarrollo, no código de Audit):

```powershell
Copy-Item "$HOME\vetcare-ev3\ms-Audit\mvnw.cmd" "$HOME\vetcare-ev3\ms-vetcare-kafka-admin\"
Copy-Item "$HOME\vetcare-ev3\ms-Audit\.mvn" "$HOME\vetcare-ev3\ms-vetcare-kafka-admin\" -Recurse -Force
```

### Probar compilación y tests

```powershell
cd "$HOME\vetcare-ev3\ms-vetcare-kafka-admin"
.\mvnw.cmd clean test
```

### Ejecutar localmente

**No colocar la contraseña en el código ni en Git.** Desde PowerShell:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
$env:VETCARE_ADMIN_USERNAME = "vetcare-admin"
$secure = Read-Host "Contraseña local de administración (mínimo 12 caracteres)" -AsSecureString
$env:VETCARE_ADMIN_PASSWORD = [System.Net.NetworkCredential]::new("", $secure).Password
Remove-Variable secure
.\mvnw.cmd spring-boot:run
```

Escucha **solo en `127.0.0.1:8086`**, autenticado con **HTTP Basic** para demostración local. **No publicar directamente a Internet.** Antes de desplegar en AWS se necesita un mecanismo seguro de autorización, idealmente JWT/IDaaS de VetCare y HTTPS.

### Endpoints

Todos requieren usuario/contraseña local:

| Método | URL | Operación |
|--|--|--|
| `GET` | `/api/admin/kafka/cluster` | ID del clúster y número de brokers |
| `GET` | `/api/admin/kafka/topics` | Listar tópicos (sin internos) |
| `GET` | `/api/admin/kafka/topics/{name}` | Particiones, réplica, configuración |
| `POST` | `/api/admin/kafka/topics` | Crear tópico |
| `PATCH` | `/api/admin/kafka/topics/{name}` | Aumentar particiones / modificar configuraciones |
| `DELETE` | `/api/admin/kafka/topics/{name}` | Eliminar tópico no crítico |
| `GET` | `/api/admin/kafka/consumer-groups` | Inspeccionar grupos de consumidores |

Ejemplo de creación (ejecutar en **otra** terminal PowerShell, con las variables de credenciales cargadas también en ella):

```powershell
$pair = "$env:VETCARE_ADMIN_USERNAME`:$env:VETCARE_ADMIN_PASSWORD"
$auth = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($pair))
$headers = @{ Authorization = "Basic $auth" }
$body = @{ name = 'vetcare.admin.demo'; partitions = 3; replicationFactor = 3; configurations = @{ 'retention.ms' = '86400000'; 'cleanup.policy' = 'delete' } } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri 'http://127.0.0.1:8086/api/admin/kafka/topics' -Method Post -Headers $headers -ContentType 'application/json' -Body $body
Invoke-RestMethod -Uri 'http://127.0.0.1:8086/api/admin/kafka/consumer-groups' -Headers $headers
```

Para modificar tópico de prueba:

```powershell
$body = @{ partitions = 4; configurations = @{ 'retention.ms' = '172800000' } } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Uri 'http://127.0.0.1:8086/api/admin/kafka/topics/vetcare.admin.demo' -Method Patch -Headers $headers -ContentType 'application/json' -Body $body
```

Eliminar solo tópico de demostración:

```powershell
Invoke-RestMethod -Uri 'http://127.0.0.1:8086/api/admin/kafka/topics/vetcare.admin.demo' -Method Delete -Headers $headers
```

> Aviso: `PATCH` no es atómico cuando incluye a la vez particiones y configuraciones; Kafka puede aplicar un cambio antes de que el otro falle. Kafka **no permite reducir** particiones con esta operación. La replicación configurada al crear un tópico no puede modificarse mediante este endpoint.

## Diseño

- Cliente nativo `Admin` de Apache Kafka, administrado como bean de Spring.
- Controlador REST → servicio → Kafka Admin API.
- Validación de DTO y lista explícita de configuraciones permitidas.
- Protege de borrado los tópicos críticos `consultations.events`, `audit.timeline`, `consultations.events.audit.DLT` y los tópicos internos `__*`.
- Usuario/contraseña **obligatorios** al arrancar; mínimo 12 caracteres.
- Pruebas unitarias sin dependencia de Kafka real.

## Pendientes de la EV3 fuera de este microservicio

1. Verificar `mvnw.cmd clean test` y probar endpoints contra el clúster local de tres brokers.
2. Entregar archivos Docker Compose correctos; la rúbrica solicita **ZooKeeper y brokers**, mientras el laboratorio KRaft local es una arquitectura diferente.
3. Revisar integración real con el resto de VetCare y la autenticación JWT del despliegue.
4. Publicar repositorios y documentos de evidencia en GitHub.

### Script de evidencia automática (administración Kafka)

Con la API iniciada y las credenciales exportadas en la terminal de prueba:

```powershell
.\scripts\PruebaAdminKafka.ps1
```

El script consulta el clúster, crea un tópico temporal con tres particiones y tres réplicas, modifica la retención, consulta consumer groups y finalmente elimina el tópico de demostración. **Nunca** intenta eliminar los tópicos reales de VetCare.

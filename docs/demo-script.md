# Guion de la demo (5 escenarios · ~15 minutos)

Pantalla sugerida: **Demo UI a la izquierda** (http://localhost:5173) y **Kafka UI a
la derecha** (http://localhost:8080). Mailhog en otra pestaña
(http://localhost:8025).

---

## Antes de empezar (checklist)

```bash
# 1. Levantar todo
docker compose up --build -d

# 2. Ver que estén todos arriba (kafka-init y mongo-init deben estar "Exited (0)")
docker compose ps

# 3. (Opcional) seguir logs mientras se hace la demo
docker compose logs -f activation-api billing-service provisioning-service
```

Abrir:

- Demo UI → http://localhost:5173
- Kafka UI → http://localhost:8080 (cluster `local`)
- Mailhog → http://localhost:8025

Si algo quedó raro de una demo anterior:

```bash
docker compose down -v
docker compose up --build -d
```

> `kafka-init` y `mongo-init` terminan en **Exited (0)**. Son trabajos de
> inicialización, no servidores: es el comportamiento esperado.
>
> En **Git Bash** (Windows), los comandos que usan rutas absolutas dentro del
> contenedor (`/opt/kafka/bin/...`) pueden necesitar `MSYS_NO_PATHCONV=1`
> adelante. En PowerShell no hace falta.

---

## Escenario 1 · Camino feliz (fan-out)

**Qué demuestra:** un evento, varias reacciones en paralelo, sin llamadas directas.

1. En la UI completar:
   - Cliente: `C-1234`
   - Plan: `PLAN DIGITAL`
   - Fallo simulado: `Sin fallo (camino feliz)`
2. Clic en **Contratar plan**.
3. Resultado esperado:
   - La API responde `202 Accepted` al instante y la activación aparece como
     **Pendiente**.
   - La timeline muestra, en orden:
     1. Activación solicitada
     2. Cuenta de facturación creada
     3. Aprovisionamiento completado (1-3 s después)
     4. Activación completada
   - El badge pasa a **Activa**.
   - En Mailhog llega el email **“Tu plan PLAN DIGITAL está activo”**.
4. En Kafka UI:
   - `Topics → activation.requested → Messages`: hay **un** mensaje.
   - `Consumers → billing-service-group` y
     `Consumers → provisioning-service-group`: ambos grupos tienen el mismo
     mensaje. Eso es el **fan-out**: cada grupo recibe su propia copia.

**Frase para el oral:** “Billing y provisioning no se conocen. Los dos están
suscritos al mismo topic en grupos distintos; Kafka entrega una copia a cada
grupo y trabajan en paralelo.”

---

## Escenario 2 · Fallo y compensación (Saga)

**Qué demuestra:** no hay rollback distribuido; hay una **transacción de
compensación**.

1. Completar:
   - Cliente: `C-777`
   - Plan: `PLAN PREMIUM`
   - Fallo simulado: `Fallo en provisioning`
2. Contratar.
3. Resultado esperado (timeline):
   1. Activación solicitada
   2. Cuenta de facturación creada
   3. Aprovisionamiento fallido
   4. Activación fallida
   5. **Cuenta anulada (compensación)**
   - Badge **Fallida**.
   - Mailhog: email **“No pudimos activar tu plan PLAN PREMIUM”**.
4. En Kafka UI:
   - `activation.events` contiene `ActivationFailed`.
   - `billing.events` contiene `BillingAccountCancelled`.
   - En `billing_db.billing_accounts`, la cuenta quedó `CANCELLED`.

**Frase para el oral:** “Billing ya había creado la cuenta. Cuando llega
`ActivationFailed`, ejecuta una compensación: marca la cuenta como cancelada y
publica `BillingAccountCancelled`. No se deshace con un rollback de base de
datos, se deshace con otro evento.”

> Probar también **Fallo en billing**: en ese caso no se crea cuenta, se publica
> `BillingFailed` y la activación pasa a `FAILED`. No hay nada que compensar.

---

## Escenario 3 · Servicio caído (offsets y lag)

**Qué demuestra:** los mensajes no se pierden; el consumidor retoma desde el
offset confirmado.

1. Detener notification:

```bash
docker compose stop notification-service
```

2. Crear **3 activaciones** desde la UI (sin fallo).
3. En Kafka UI → `Consumers → notification-service-group`:
   - El **lag** es 3 (o mirar `Messages` pendientes).
   - Las activaciones igual terminan `ACTIVE`: notification es independiente.
4. Volver a levantar:

```bash
docker compose up -d notification-service
```

5. Resultado esperado:
   - Llegan los **3 emails** a Mailhog.
   - El lag vuelve a 0.

Comando alternativo para mostrar el lag por consola:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

**Frase para el oral:** “El offset se confirma después de procesar. Si el
servicio está caído, Kafka conserva los mensajes y el grupo sigue desde el último
offset confirmado. No se pierde ninguno porque la entrega es *at-least-once*.”

---

## Escenario 4 · Escalar consumidores (consumer groups)

**Qué demuestra:** Kafka reparte las particiones entre instancias del mismo
grupo. No hay que cambiar código.

1. Escalar billing a 2 instancias:

```bash
docker compose up -d --scale billing-service=2
```

2. Crear 2 o 3 activaciones.
3. En Kafka UI → `Consumers → billing-service-group`:
   - Hay **2 miembros** (`billing-service-...`).
   - Las 3 particiones de `activation.requested` y las 3 de `activation.events`
     se reparten entre los dos (cada partición la consume **un solo** miembro).
4. Volver a una instancia:

```bash
docker compose up -d --scale billing-service=1
```

**Frase para el oral:** “Un consumer group es un grupo de consumidores que se
reparten las particiones. Como máximo hay un consumidor por partición; si agrego
instancias, Kafka rebalancea. Con 3 particiones, la cuarta instancia quedaría
ociosa.”

---

## Escenario 5 · Replay del log (event sourcing)

**Qué demuestra:** el log de Kafka es inmutable y se puede releer desde el
principio con un consumidor nuevo.

1. Levantar el servicio de fidelización (perfil opcional):

```bash
docker compose --profile loyalty up -d loyalty-service
```

2. Ver los logs:

```bash
docker compose --profile loyalty logs loyalty-service
```

Resultado esperado:

- `loyalty-service` procesa **todas** las `ActivationCompleted` históricas,
  aunque se hayan producido antes de que el servicio existiera.
- Por cada una: `+10 puntos para C-XXXX ... (replay del log)`.
- En MongoDB, base `loyalty_db`, colección `loyalty_accounts`: puntos por cliente.

3. Reiniciar el servicio y mostrar que **no duplica** puntos (offsets ya
   confirmados + idempotencia):

```bash
docker compose --profile loyalty restart loyalty-service
```

**Frase para el oral:** “El grupo `loyalty-service-group` es nuevo, no tiene
offsets. Con `fromBeginning: true` (equivale a `auto.offset.reset=earliest`) Kafka
le entrega todo el historial. Es algo que con APIs REST directas no se puede
hacer.”

Para apagarlo (opcional):

```bash
docker compose --profile loyalty stop loyalty-service
```

### Replay del CRM (mismo concepto, RF-10)

El CRM ya leyó los eventos, así que su grupo tiene offsets. Para comprobar que
puede **releer el historial completo**, se lo ejecuta una vez con un `groupId`
nuevo:

```bash
docker compose run --rm -e KAFKA_GROUP_ID=crm-replay-group crm-analytics-service
```

Los logs muestran `[IDEMPOTENCIA] ... Evento duplicado ...: se descarta`, lo que
demuestra dos cosas: el grupo nuevo leyó desde el inicio y la idempotencia evita
duplicar la auditoría. Se detiene con `Ctrl+C`.

> Para repetir la prueba hay que usar **otro** nombre de grupo (por ejemplo
> `crm-replay-group-2`): un grupo que ya leyó conserva sus offsets.

---

## Bonus A · Demostrar idempotencia (si sobra tiempo)

Reprocesamos los mismos mensajes de notification y verificamos que **no se
duplican los emails**:

```bash
# 1. Detener el consumidor (el grupo debe estar inactivo para resetear offsets)
docker compose stop notification-service

# 2. Volver el grupo de notification al inicio del topic
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group notification-service-group \
  --topic activation.events \
  --reset-offsets --to-earliest --execute

# 3. Volver a levantar
docker compose up -d notification-service
```

En los logs aparece:

```text
[IDEMPOTENCIA] [notification-service] Evento duplicado ...: se descarta sin repetir efectos.
```

Y la cantidad de emails en Mailhog **no cambia**.

---

## Bonus B · Demostrar la DLQ

```bash
# Publicar un evento inválido (sin eventId) en billing.events
echo 'C-999:{"foo":"bar"}' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic billing.events \
  --property parse.key=true --property key.separator=:
```

Esperar ~10 segundos (3 reintentos: 1 s + 2 s + 4 s) y mirar:

- Logs: `[REINTENTO] ... Intento 3/3` y `[DLQ] ... Se publica en billing.events.dlq`.
- Kafka UI → topic `billing.events.dlq`: el mensaje con el error en los headers
  (`x-error`, `x-original-event-id`).

El resto del flujo sigue funcionando: un mensaje roto **no bloquea la partición**.

---

## Cierre (1 minuto)

Recorrido sugerido para cerrar el oral:

1. Mostrar la arquitectura en `docs/architecture.md`.
2. Nombrar los 5 escenarios y relacionarlos con los conceptos: fan-out, Saga,
   offsets, consumer groups y replay.
3. Mencionar las limitaciones conocidas: un broker, sin Outbox, sin auth.
4. Proponer mejoras: Outbox, Schema Registry, 3 brokers, trazas distribuidas,
   métricas y alertas de lag.

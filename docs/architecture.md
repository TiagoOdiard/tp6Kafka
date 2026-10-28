# Arquitectura de la POC

Este documento explica **qué hace el sistema, dónde está cada cosa y por qué se
tomó cada decisión**. Está pensado como material de estudio para la defensa oral.

---

## 1. Idea central

Un cliente contrata un plan desde la UI. `activation-api` guarda la activación y
publica **un solo evento** (`ActivationRequested`) en Kafka. A partir de ahí,
`billing-service` y `provisioning-service` reaccionan **en paralelo y sin
conocerse**. Cada uno publica su resultado. `activation-api` escucha ambos
resultados y decide el estado final de la Saga.

Esto es **coreografía con agregador**:

- **Coreografía:** nadie da órdenes; los servicios reaccionan a hechos.
- **Agregador:** `activation-api` no dirige el flujo, solo junta los resultados
  y decide cuándo la Saga terminó.

```text
                         ┌───────────────────┐
   POST /activations     │   activation-api  │◄──────────────┐
   (202 Accepted) ──────►│  REST + WebSocket │               │
                         │  + agregador Saga │               │
                         └─────────┬─────────┘               │
                                   │ ActivationRequested      │ BillingAccountCreated
                                   ▼                          │ ProvisioningCompleted
                         ┌───────────────────┐                │
                         │       KAFKA       │                │
                         │  4 topics + DLQ   │                │
                         └───┬───────────┬───┘                │
                             │           │                    │
              ┌──────────────▼──┐   ┌────▼──────────────┐     │
              │ billing-service │   │ provisioning-svc  │─────┘
              │  crea/cancela   │   │  simula 1-3 seg   │
              └────────┬────────┘   └────────┬──────────┘
                       │ BillingAccount...   │ Provisioning...
                       └─────────────────────┘
                                   │
                   ┌───────────────┼──────────────────┐
                   ▼               ▼                  ▼
          notification-svc   crm-analytics-svc   loyalty-svc (opcional)
             (Mailhog)        (audita todo)      (replay del log)
```

---

## 2. Componentes

| Componente | Responsabilidad | Archivo principal |
|---|---|---|
| `activation-api` | Punto de entrada REST, WebSocket, estado de la Saga y publicación de eventos finales | `apps/activation-api/src/activation/activation.service.ts` |
| `billing-service` | Crea la cuenta simulada y la cancela cuando la activación falla | `apps/billing-service/src/billing/billing.service.ts` |
| `provisioning-service` | Simula el aprovisionamiento con 1-3 s de demora y puede fallar a pedido | `apps/provisioning-service/src/provisioning/provisioning.service.ts` |
| `notification-service` | Envía el email de bienvenida o de error a Mailhog | `apps/notification-service/src/notification/notification.service.ts` |
| `crm-analytics-service` | Consumidor pasivo: guarda una copia de todos los eventos | `apps/crm-analytics-service/src/crm/crm.consumer.ts` |
| `loyalty-service` | Consumidor nuevo que reprocesa el historial (escenario 5) | `apps/loyalty-service/src/loyalty/loyalty.consumer.ts` |
| `demo-ui` | Formulario y timeline en vivo por WebSocket | `apps/demo-ui/src/App.tsx` |
| `libs/contracts` | Nombres de topics/eventos, sobre común y tipos de payload | `libs/contracts/src/index.ts` |
| `libs/kafka-toolkit` | Envelope, publisher con clave, idempotencia, reintentos y DLQ | `libs/kafka-toolkit/src/index.ts` |

---

## 3. Topics

Todos tienen **3 particiones**, **retención de 7 días** y usan `customerId`
como **clave del mensaje**. Sus DLQ son `<topic>.dlq`.

| Topic | Eventos | Productor | Consumidores |
|---|---|---|---|
| `activation.requested` | `ActivationRequested` | activation-api | billing, provisioning, crm |
| `billing.events` | `BillingAccountCreated`, `BillingFailed`, `BillingAccountCancelled` | billing | activation-api, crm |
| `provisioning.events` | `ProvisioningCompleted`, `ProvisioningFailed` | provisioning | activation-api, crm |
| `activation.events` | `ActivationCompleted`, `ActivationFailed` | activation-api | notification, billing, crm, loyalty |

Los topics se crean en `docker/create-topics.sh` (contenedor `kafka-init`).

---

## 4. Sobre común de los eventos (Envelope)

Definido en `libs/contracts/src/index.ts`:

```json
{
  "eventId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "eventType": "ActivationRequested",
  "version": 1,
  "occurredAt": "2026-09-27T16:40:00Z",
  "correlationId": "act-0001",
  "customerId": "C-1234",
  "source": "activation-api",
  "payload": {
    "planId": "PLAN DIGITAL",
    "channel": "web",
    "simulateFailure": "none"
  }
}
```

| Campo | Para qué sirve |
|---|---|
| `eventId` | Identificador único del evento. Es la base de la **idempotencia**. |
| `eventType` + `version` | Permiten evolucionar el contrato sin romper consumidores. |
| `correlationId` | Es el `activationId`: agrupa todos los eventos de una activación. |
| `customerId` | Es la **clave de Kafka**: define la partición y garantiza orden por cliente. |
| `source` | Servicio que publicó el evento. |
| `occurredAt` | Momento del hecho (ISO 8601). |

Convenciones:

- Los eventos se nombran en **pasado** porque describen hechos consumados.
- Los topics usan `dominio.tipo`.
- Un cambio compatible agrega campos opcionales; uno incompatible sube `version`.

---

## 5. Flujo completo (camino feliz)

```text
UI                activation-api        Kafka              billing          provisioning
│ POST /activations      │                 │                   │                  │
├───────────────────────►│                 │                   │                  │
│                        │ guarda PENDING  │                   │                  │
│                        ├─ ActivationRequested ──────────────►│                  │
│                        │                 ├──────────────────────────────────────►│
│◄── 202 {id, PENDING} ──┤                 │                   │                  │
│                        │                 │◄─ BillingAccountCreated ─┤           │
│                        │◄────────────────┤                   │                  │
│                        │ status IN_PROGRESS                  │                  │
│                        │                 │                   │  (espera 1-3 s)  │
│                        │                 │◄─ ProvisioningCompleted ───────────────┤
│                        │◄────────────────┤                   │                  │
│                        │ status ACTIVE   │                   │                  │
│                        ├─ ActivationCompleted ──────────────►│ (no le interesa) │
│                        │                 ├──────────────────────────────────────►│
│◄─ WS: 4 eventos ───────┤                 │                   │                  │
```

Puntos importantes:

1. El `POST` responde **202 Accepted** sin esperar a nadie (RF-02).
2. `activation-api` guarda el resultado parcial de cada paso en
   `steps.billing` y `steps.provisioning`.
3. La transición a `ACTIVE` se hace con un `findOneAndUpdate` **condicional**
   (`status: { $nin: ['ACTIVE','FAILED'] }`) para que dos resultados concurrentes
   no publiquen el cierre dos veces.
4. Recién cuando el estado cambió se publica `ActivationCompleted`.

## 6. Flujo de fallo y compensación

```text
provisioning falla
   ├─ publica ProvisioningFailed
   ├─ activation-api guarda steps.provisioning = FAILED
   ├─ activation-api cambia a FAILED
   ├─ publica ActivationFailed
   └─ billing escucha ActivationFailed
        ├─ guarda la intención de cancelar
        └─ si la cuenta existe: CANCELLED + BillingAccountCancelled
```

**No hay rollback distribuido.** No existe una transacción ACID que abarque dos
microservicios. Se usa el patrón **Saga con transacción de compensación**: se
deshace el efecto ya confirmado (anular la cuenta) mediante otro evento.

### Caso borde (documentado en el PDF)

`ActivationFailed` puede llegar a billing **antes** de que termine de procesar
`ActivationRequested`. Solución en `billing.service.ts`:

1. Al recibir `ActivationFailed`, guarda la intención en la colección
   `cancellations` (con `_id = activationId`).
2. Si la cuenta ya existe, la cancela.
3. Cuando más tarde procesa `ActivationRequested` y crea la cuenta, **vuelve a
   consultar `cancellations`** y la cancela en el acto.

Así, sin importar el orden de llegada, la cuenta termina cancelada.

---

## 7. Estados de la activación

```text
PENDING ──(primer resultado)──► IN_PROGRESS ──(ambos OK)──► ACTIVE
   │                                  │
   └──────────(algún fallo)───────────┴──────────────────► FAILED
```

Los eventos que llegan **después** de un estado final no cambian el estado:
solo se agregan al historial (tal como pide el enunciado).

---

## 8. Modelo de datos (MongoDB)

Una base por servicio; ningún servicio lee la base de otro.

| Base | Colección | Contenido |
|---|---|---|
| `activation_db` | `activations` | Estado, pasos, historial |
| `activation_db` | `processed_events` | Idempotencia |
| `billing_db` | `billing_accounts` | Cuenta `CREATED`/`CANCELLED` |
| `billing_db` | `cancellations` | Intención de cancelar (caso borde) |
| `provisioning_db` | `provisioning_orders` | Orden técnica simulada |
| `notification_db` | `notifications` | Emails enviados |
| `crm_db` | `event_log` | Copia de todos los eventos + topic/partición/offset |
| `loyalty_db` | `loyalty_accounts` | Puntos por cliente (replay) |

Documento de `activations` (ejemplo):

```json
{
  "_id": "act-0001",
  "customerId": "C-1234",
  "planId": "PLAN DIGITAL",
  "status": "ACTIVE",
  "simulateFailure": "none",
  "steps": {
    "billing":      { "status": "OK", "at": "2026-09-27T16:40:02Z" },
    "provisioning": { "status": "OK", "at": "2026-09-27T16:40:03Z" }
  },
  "history": [
    { "eventId": "...", "eventType": "ActivationRequested",     "source": "activation-api",        "at": "..." },
    { "eventId": "...", "eventType": "BillingAccountCreated",   "source": "billing-service",       "at": "..." },
    { "eventId": "...", "eventType": "ProvisioningCompleted",   "source": "provisioning-service",  "at": "..." },
    { "eventId": "...", "eventType": "ActivationCompleted",     "source": "activation-api",        "at": "..." }
  ]
}
```

El historial se agrega con un filtro `'history.eventId': { $ne: eventId }` para
que un reintento no lo duplique.

---

## 9. Cómo se cumple cada requerimiento no funcional

| RNF | Requerimiento | Dónde está | Cómo funciona |
|---|---|---|---|
| RNF-01 | Todo se levanta con `docker compose up` | `docker-compose.yml` | Healthchecks + `depends_on` con `service_healthy` / `service_completed_successfully`. |
| RNF-02 | Orden por cliente | `libs/kafka-toolkit/src/index.ts` → `KafkaPublisher` | Se publica con `key = customerId`; Kafka manda la misma clave a la misma partición. |
| RNF-03 | Consumidores idempotentes | `checkIdempotency` / `runIdempotent` | Se inserta el `eventId` en `processed_events` (índice único). Si ya existe, se descarta. |
| RNF-04 | 3 reintentos y DLQ | `withRetryAndDlq` | Espera 1 s, 2 s y 4 s. Si falla, publica en `<topic>.dlq` y sigue. |
| RNF-05 | Caída y recuperación sin pérdida | `run: { autoCommit: false }` + `commitOffset` | El offset se confirma **después** de procesar. Si el servicio se cae, Kafka reentrega. |
| RNF-06 | Trazabilidad | `correlationId` | Es el `activationId`; se guarda en el historial y en `event_log`. |
| RNF-07 | Contratos versionados y compartidos | `libs/contracts` | Una única fuente de verdad, importada por todos los servicios. |
| RF-10 | Reprocesar el historial | `crm.consumer.ts`, `loyalty.consumer.ts` | Con un `groupId` nuevo y `fromBeginning: true` se lee el log desde el inicio. |

### Detalle de idempotencia

`runIdempotent` hace:

1. Intenta insertar el `eventId` en `processed_events`.
2. Si MongoDB responde `11000` (clave duplicada), el evento ya se procesó → se
   descarta y se confirma el offset.
3. Si el efecto de negocio falla, **borra la marca** y relanza el error, para que
   el reintento vuelva a ejecutarlo.

El TTL de `processed_events` es de 7 días, igual que la retención de los topics:
un evento más viejo ya no puede volver a llegar.

### Fallo de negocio vs. fallo técnico

- **Fallo de negocio simulado** (`simulateFailure`): se publica
  `BillingFailed` / `ProvisioningFailed`. **No** va a la DLQ.
- **Fallo técnico** (base caída, evento inválido, excepción): se reintenta 3 veces
  y va a la DLQ.

---

## 10. Decisiones técnicas y por qué

### ¿Por qué la clave es `customerId`?

Kafka solo garantiza orden **dentro de una partición**. Si dos eventos del mismo
cliente fueran a particiones distintas, podrían procesarse desordenados. Al usar
`customerId` como clave, todos los eventos de ese cliente van a la misma
partición y se procesan en orden.

### ¿Por qué se publica con `kafkajs` y se consume con `@nestjs/microservices`?

`@EventPattern` y `KafkaContext` permiten consumir con la estructura de NestJS y
confirmar el offset a mano. Pero `ClientKafka.emit()` usa el **nombre del patrón**
como clave del mensaje, y necesitábamos que la clave sea `customerId`. Por eso el
productor es un wrapper chico sobre `kafkajs` (`KafkaPublisher`).

### ¿Por qué commit manual?

Con `autoCommit: true` Kafka confirma el offset apenas entrega el mensaje. Si el
servicio se cae durante el procesamiento, el mensaje se pierde. Con
`autoCommit: false` y `commitOffsets` al final, se cumple **at-least-once**: si
hay una caída, el mensaje se reentrega (y la idempotencia evita efectos dobles).

### ¿Por qué no hay transacciones entre servicios?

Porque cada servicio tiene su propia base y no se puede bloquear una transacción
global. Se usa **Saga**: cada paso es una transacción local y, si algo falla, se
ejecuta una **compensación** explícita.

### ¿Por qué no hay Outbox en esta entrega?

El Outbox resuelve el caso de que `activation-api` guarde en Mongo y se caiga
antes de publicar el evento. Es la **fase 4** del enunciado (opcional, para un
piloto). El riesgo está documentado y la POC publica inmediatamente después de
guardar. La base ya corre como replica set de un nodo para poder implementarlo
sin cambiar la infraestructura.

### Otras decisiones

- **MongoDB en replica set de 1 nodo:** habilita transacciones si se implementa
  Outbox. `docker/mongo-init.sh` lo inicializa.
- **NestJS `postfixId: ''`:** sin esto, Nest agrega `-server` al `groupId`
  (`billing-service-group-server`). Lo dejamos vacío para que los nombres sean
  los documentados.
- **Un broker:** alcanza para mostrar particiones, offsets y consumer groups. No
  hay alta disponibilidad; si el broker se cae, el sistema no procesa.
- **Kafka UI:** permite ver en vivo topics, mensajes, offsets y **lag** por
  consumer group.

---

## 11. Recorrido del código (para el oral)

### 1. Entra el pedido

`apps/activation-api/src/activation/activation.controller.ts`

```ts
@Post()
@HttpCode(202)
create(@Body() dto: CreateActivationDto) { ... }
```

Responde 202 y delega en `ActivationService.create`.

### 2. Se guarda y se publica

`apps/activation-api/src/activation/activation.service.ts` → `create()`

- Genera `activationId = act-xxxxxxxx`.
- Crea el sobre con `createEnvelope`.
- Guarda el documento `PENDING` en Mongo.
- Publica en `activation.requested` con clave `customerId`.
- Avisa por WebSocket.

### 3. Reaccionan billing y provisioning

- `billing.service.ts` → `handleActivationRequested()`
- `provisioning.service.ts` → `handleActivationRequested()`

Ambos son consumidores del **mismo topic**, en **grupos distintos**. Kafka
entrega una copia a cada grupo: por eso trabajan en paralelo (fan-out).

### 4. Se juntan los resultados

`activation.service.ts` → `handleStepEvent()`

- Agrega el evento al historial.
- Guarda el resultado del paso si es la primera vez.
- Si hay fallo: `finishFailed()`.
- Si ambos pasos están OK: `finishActive()`.

`finishActive` / `finishFailed` usan `findOneAndUpdate` condicional y publican
`ActivationCompleted` / `ActivationFailed`.

### 5. Consumo confiable

`activation.consumer.ts` (el patrón se repite en todos los servicios):

```ts
await withRetryAndDlq({
  operation: () => this.process(topic, envelope),
  ...
});
finally { await this.commitOffset(context); }
```

`process` valida el sobre y llama a `runIdempotent`.

### 6. Notificación

`notification.service.ts` arma el email, lo envía por SMTP a Mailhog y guarda el
registro en `notifications`.

### 7. UI en vivo

`activation.gateway.ts` emite `activation.event` por Socket.io.
`apps/demo-ui/src/App.tsx` escucha ese evento y arma la timeline.

---

## 12. Concurrencia: por qué usamos updates condicionales

Los resultados de billing y provisioning pueden llegar casi al mismo tiempo.
Si el código hiciera “leer → decidir → escribir”, dos procesos podrían decidir
lo mismo y publicar dos veces el cierre.

La transición usa una condición atómica en MongoDB:

```ts
findOneAndUpdate(
  { _id, status: { $nin: ['ACTIVE', 'FAILED'] } },
  { $set: { status: 'ACTIVE' } },
)
```

Solo **un** proceso obtiene el documento; el otro recibe `null` y no publica
nada. Es una forma simple de exclusión mutua optimista.

---

## 13. Limitaciones conocidas (decirlas en el oral suma)

- Un solo broker: no hay tolerancia a fallos del broker ni réplicas.
- Sin Outbox: si `activation-api` se cae entre el guardado y la publicación, el
  evento se pierde (riesgo documentado en la fase 4).
- La idempotencia no es transaccional con el efecto: si el proceso muere entre la
  marca y el efecto, el reintento no repite el efecto. En un piloto se resuelve
  con transacciones de MongoDB (el replica set ya está configurado).
- Sin autenticación ni autorización.
- Sin Schema Registry ni trazas distribuidas (fase opcional).

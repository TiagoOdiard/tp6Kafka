# Guía de defensa oral

Material para entender el código y responder preguntas. No hace falta memorizar:
la idea es poder explicar **qué problema resuelve cada pieza**.

---

## 1. Pitch de 60 segundos

> “Es una POC de activación de servicios con Kafka. La UI hace un POST a
> `activation-api`, que guarda la activación en MongoDB y publica un único evento,
> `ActivationRequested`, en Kafka. Ese evento lo consumen en paralelo
> `billing-service` y `provisioning-service`, cada uno en su propio consumer
> group. Cada servicio publica su resultado y `activation-api` actúa como
> agregador de la Saga: cuando los dos confirman, pasa la activación a `ACTIVE` y
> publica `ActivationCompleted`; si alguno falla, pasa a `FAILED`, publica
> `ActivationFailed` y billing compensa anulando la cuenta. Todo el flujo es
> idempotente, tiene reintentos con DLQ y confirmación manual de offsets. La UI
> recibe los eventos en vivo por WebSocket.”

---

## 2. Glosario con analogías

| Concepto | Explicación corta | Analogía |
|---|---|---|
| **Topic** | Categoría o “canal” donde se publican mensajes de un tipo. | Una carpeta o un canal de TV. |
| **Partición** | División ordenada de un topic. Permite paralelismo. | Varias cajas del mismo mostrador. |
| **Offset** | Número de posición de cada mensaje dentro de una partición. | El número de ticket. |
| **Clave (key)** | Valor que decide la partición. Usamos `customerId`. | El DNI decide en qué ventanilla te atienden. |
| **Consumer group** | Grupo de consumidores que se reparten las particiones. | Varios empleados repartiéndose cajas. |
| **Lag** | Mensajes pendientes de procesar en un grupo. | Pedidos en la bandeja de entrada. |
| **Commit de offset** | Confirmar “ya procesé hasta acá”. | Firmar el remito. |
| **At-least-once** | Un mensaje puede entregarse más de una vez. | Un cartero que a veces toca dos veces. |
| **Idempotencia** | Procesar dos veces el mismo mensaje produce el mismo efecto que una. | Pulsar dos veces “encender” no enciende dos veces. |
| **Saga** | Secuencia de transacciones locales con compensaciones. | Un viaje con pasos que se pueden deshacer. |
| **Compensación** | Acción que deshace el efecto de un paso ya confirmado. | Anular una reserva en vez de “rebobinar” el tiempo. |
| **DLQ** | Topic donde se derivan los mensajes que fallaron definitivamente. | Bandeja de “no se pudo entregar”. |
| **Replay** | Volver a leer el log desde el inicio. | Rebobinar una película. |

---

## 3. Recorrido del flujo (para dibujar en el pizarrón)

```text
UI ──POST──► activation-api ──ActivationRequested──► Kafka
                                                        │
                        ┌───────────────────────────────┤
                        ▼                               ▼
                 billing-service                provisioning-service
                        │                               │
             BillingAccountCreated            ProvisioningCompleted
                        └──────────────┬────────────────┘
                                       ▼
                               activation-api
                            (agregador de la Saga)
                                       │
                          ┌────────────┴────────────┐
                          ▼                         ▼
                  ActivationCompleted        ActivationFailed
                          │                         │
                 notification + CRM        billing compensa
```

Estados: `PENDING → IN_PROGRESS → ACTIVE` o `FAILED`.

---

## 4. Preguntas probables y respuestas

### Kafka y mensajería

**¿Qué es Kafka y en qué se diferencia de una cola tradicional?**

Kafka es un **log distribuido y persistente**. Los mensajes se agregan al final
de una partición y quedan retenidos (7 días en esta POC), aunque ya se hayan
consumido. Una cola tradicional (RabbitMQ, por ejemplo) borra el mensaje cuando
el consumidor hace ack y no permite releer el historial. Esa persistencia es la
que habilita el replay del escenario 5.

**¿Por qué usaron Kafka y no REST entre servicios?**

Porque con REST el emisor conoce a los receptores, hay acoplamiento temporal (si
el otro no está, falla) y agregar un consumidor nuevo obliga a tocar al emisor.
Con Kafka el productor publica un hecho y no sabe quién lo consume. Agregar
`crm-analytics` o `loyalty` no requirió cambiar a nadie. Además, Kafka guarda el
historial y permite reprocesarlo.

**¿Qué garantiza Kafka sobre el orden?**

Solo garantiza orden **dentro de una partición**. Por eso usamos `customerId` como
clave: todos los eventos del mismo cliente caen en la misma partición y se
procesan en orden. Eventos de clientes distintos pueden procesarse en paralelo.

**¿Qué es un consumer group?**

Es un grupo lógico de consumidores que se reparten las particiones de un topic.
Cada partición la consume **un solo** miembro del grupo. Si hay más consumidores
que particiones, los sobrantes quedan ociosos. Distintos grupos reciben cada uno
su propia copia del mensaje: eso permite que billing y provisioning procesen el
mismo evento en paralelo.

**¿Qué pasa si se cae un consumidor?**

Kafka detecta la caída (heartbeat/session timeout), hace **rebalanceo** y asigna
sus particiones a los miembros vivos. El nuevo consumidor retoma desde el último
offset confirmado del grupo.

**¿Qué es el lag?**

La diferencia entre el último offset producido y el último offset confirmado por
el grupo. Si el consumidor está caído, el lag crece; cuando vuelve, se procesa lo
pendiente. En el escenario 3 se ve en Kafka UI.

**¿At-least-once, at-most-once o exactly-once?**

Nuestra POC es **at-least-once**: puede haber duplicados, y los resolvemos con
idempotencia. Exactly-once requeriría transacciones de Kafka (`read_committed`) o
un diseño idempotente como el nuestro. At-most-once perdería mensajes.

**¿Por qué confirmar el offset a mano?**

Porque `autoCommit` confirma apenas se entrega el mensaje. Si el servicio se cae
durante el procesamiento, ese mensaje se pierde. Con `autoCommit: false` y
`commitOffsets` al final, si hay una caída el mensaje se reentrega. El costo es
que puede haber duplicados, y por eso está la idempotencia.

### Arquitectura y Saga

**¿Qué es una Saga?**

Un patrón para manejar transacciones distribuidas sin una transacción ACID global.
Cada servicio hace su transacción local y, si algo falla, se ejecutan
**compensaciones** explícitas. En nuestra Saga: billing crea la cuenta; si la
activación falla, billing la cancela.

**¿Por qué no usaron two-phase commit (2PC)?**

Porque 2PC requiere coordinación y bloqueos entre servicios, es difícil de
escalar y acopla a todos los participantes. La Saga es más simple y resiliente:
cada servicio avanza y se compensa si hace falta.

**Coreografía vs. orquestación: ¿qué eligieron?**

Coreografía con agregador. Billing y provisioning reaccionan solos al evento; no
hay un orquestador que les diga “ahora facturá, ahora aprovisioná”.
`activation-api` solo junta los resultados y decide el cierre. Elegimos esto
porque muestra mejor el desacople.

**¿Quién decide que la activación terminó?**

`activation-api`, en `handleStepEvent`. Guarda el resultado de cada paso. Si los
dos están `OK`, publica `ActivationCompleted`; si alguno está `FAILED`, publica
`ActivationFailed`. Los eventos posteriores a un estado final solo se registran
en el historial.

**¿Qué pasa si billing y provisioning responden al mismo tiempo?**

Puede pasar. Por eso las transiciones usan `findOneAndUpdate` con la condición
`status: { $nin: ['ACTIVE','FAILED'] }`. Solo un proceso logra cambiar el estado;
el otro no publica nada. Es exclusión mutua optimista.

**¿Cómo se compensa si billing ya creó la cuenta?**

Billing escucha `ActivationFailed`, marca la cuenta como `CANCELLED` y publica
`BillingAccountCancelled`. No se borra el documento: se registra el cambio de
estado (auditoría).

**¿Y si `ActivationFailed` llega antes de que billing cree la cuenta?**

Billing guarda primero la **intención de cancelar** en la colección
`cancellations`. Cuando después procesa `ActivationRequested` y crea la cuenta,
consulta esa colección y la cancela inmediatamente. Así el orden de llegada no
importa.

**¿Qué pasa con los emails cuando falla la activación?**

`notification-service` consume `activation.events` y manda un email de error con
el motivo. También guarda el envío en `notifications`.

### Confiabilidad

**¿Qué pasa si llega dos veces el mismo evento?**

Cada consumidor inserta el `eventId` en `processed_events`, que tiene índice
único. La segunda inserción falla con código `11000` (duplicate key), el evento se
descarta y se confirma el offset. No se duplican cuentas, emails ni puntos.

**¿Por qué no alcanza con “si ya existe la cuenta, no la creo”?**

Porque no todos los efectos son naturalmente idempotentes. Un email o un
contador de puntos sí se duplicaría. La marca por `eventId` es genérica y sirve
para cualquier consumidor.

**¿Qué pasa si el efecto de negocio falla después de insertar la marca?**

`runIdempotent` borra la marca y relanza el error, para que el reintento vuelva a
ejecutar el efecto. Si el proceso muere de golpe entre la marca y el efecto, la
marca queda y el efecto se pierde; esa es una limitación conocida y se resolvería
con transacciones de MongoDB.

**¿Qué pasa con un mensaje inválido o una base caída?**

Es un **fallo técnico**. `withRetryAndDlq` reintenta 3 veces con espera de 1 s,
2 s y 4 s. Si sigue fallando, publica el mensaje original en `<topic>.dlq` con el
error en los headers y continúa con el siguiente mensaje. Así no se bloquea la
partición.

**¿Un fallo de negocio va a la DLQ?**

No. `simulateFailure` es un fallo de negocio: se publica `BillingFailed` o
`ProvisioningFailed` como un evento normal. La DLQ es solo para errores técnicos.

**¿Por qué el TTL de `processed_events` es de 7 días?**

Porque es la retención de los topics. Un evento más viejo que eso ya no puede
volver a entregarse, así que la marca ya no es necesaria.

**¿Qué pasa si Kafka está caído cuando se hace el POST?**

El documento se guarda en Mongo y la publicación falla; la API devuelve error y
la activación queda `PENDING`. Para evitar esa ventana se usa el patrón
**Transactional Outbox** (fase 4): guardar el evento en una colección `outbox` en
la misma transacción que la activación, y un proceso aparte lo publica y lo marca
como enviado.

### Código y decisiones

**¿Por qué el productor usa `kafkajs` y no `ClientKafka.emit`?**

Porque `ClientKafka.emit` usa el nombre del patrón como clave del mensaje. Nosotros
necesitamos `customerId` como clave para cumplir el orden por cliente. El
consumo sí usa `@EventPattern` y `KafkaContext` de NestJS.

**¿Dónde está la lógica de idempotencia?**

En `libs/kafka-toolkit/src/index.ts`, funciones `checkIdempotency` y
`runIdempotent`. La usan todos los consumidores.

**¿Dónde está la lógica de reintentos y DLQ?**

En `libs/kafka-toolkit/src/index.ts`, función `withRetryAndDlq`. Cada consumidor
le pasa la operación a ejecutar y una función para publicar en la DLQ.

**¿Por qué los contratos están en una librería?**

Para tener una única fuente de verdad de nombres de topics, tipos de eventos y
estructura del sobre. Si cambia un contrato, se cambia una vez y todos los
servicios compilan contra la misma versión.

**¿Qué es el `correlationId`?**

Es el identificador que agrupa todos los eventos de una misma activación. En
nuestra POC es el `activationId`. Sirve para rastrear el flujo de punta a punta
en los logs y en la base.

**¿Qué significa el 202?**

`202 Accepted`: el pedido fue aceptado para procesarse, pero todavía no terminó.
Es correcto porque la activación es asincrónica: la API no espera a billing ni a
provisioning.

**¿Cómo se actualiza la UI?**

`activation-api` tiene un gateway de Socket.io. Cada vez que se agrega un evento
al historial o cambia el estado, emite `activation.event` con el evento, el
estado y los pasos. La UI escucha y arma la timeline. También puede reconstruirla
con `GET /activations/:id`.

**¿Por qué una base por servicio?**

Porque cada microservicio es dueño de sus datos. Nadie lee la base de otro: se
comunica por eventos. Eso reduce el acoplamiento y permite cambiar el esquema
interno sin romper a los demás.

**¿Por qué MongoDB?**

Porque el documento de activación tiene estructura variable (pasos opcionales,
historial de eventos) y MongoDB lo modela naturalmente. Además el replica set de
un nodo permite transacciones si se implementa Outbox.

**¿Por qué `postfixId: ''` en la configuración de Kafka?**

Porque NestJS, por defecto, agrega `-server` al `groupId`
(`billing-service-group-server`). Lo vaciamos para que el grupo tenga exactamente
el nombre documentado y sea fácil verlo en Kafka UI.

### Operación y mejoras

**¿Cómo escalarían un consumidor?**

`docker compose up -d --scale billing-service=2`. Kafka rebalancea y reparte las
particiones. El límite es la cantidad de particiones: con 3, la cuarta instancia
queda ociosa.

**¿Qué pasa si agregan un consumidor nuevo?**

Se suscribe con un `groupId` nuevo. Si se configura `fromBeginning: true`, lee
todo el historial sin afectar a los grupos existentes. Eso se demuestra con
`loyalty-service`.

**¿Cómo se reprocesa el historial del CRM si ya leyó los eventos?**

Con un `groupId` nuevo: `docker compose run --rm -e KAFKA_GROUP_ID=crm-replay-group
crm-analytics-service`. El grupo anterior conserva sus offsets; el nuevo arranca
desde el inicio. Eso es lo que permite reprocesar sin duplicar en los demás.

**¿Qué limitaciones tiene la POC?**

Un solo broker (sin alta disponibilidad), sin Outbox implementado, sin auth, sin
Schema Registry ni trazas distribuidas, y la idempotencia no es transaccional. En
producción se agregarían réplicas, Outbox, monitoreo de lag, alertas y
observabilidad.

**¿Qué mejorarían primero?**

1. Outbox para no perder eventos si se cae la API entre guardar y publicar.
2. Transacción Mongo para idempotencia + efecto.
3. Tres brokers con replicación.
4. Métricas y alertas de lag.

---

## 5. Mapa rápido: “¿dónde está…?”

| Si preguntan por… | Archivo |
|---|---|
| Endpoint `POST /activations` | `apps/activation-api/src/activation/activation.controller.ts` |
| Creación y publicación inicial | `apps/activation-api/src/activation/activation.service.ts` (`create`) |
| Agregación de la Saga | `.../activation.service.ts` (`handleStepEvent`, `finishActive`, `finishFailed`) |
| Consumo con reintentos/DLQ | `.../activation.consumer.ts` (patrón en todos los servicios) |
| WebSocket | `apps/activation-api/src/gateway/activation.gateway.ts` |
| Envelope y publisher | `libs/kafka-toolkit/src/index.ts` |
| Idempotencia | `libs/kafka-toolkit/src/index.ts` (`runIdempotent`) |
| Reintentos y DLQ | `libs/kafka-toolkit/src/index.ts` (`withRetryAndDlq`) |
| Topics y eventos | `libs/contracts/src/index.ts` |
| Compensación | `apps/billing-service/src/billing/billing.service.ts` (`cancelAccount`) |
| Caso borde de orden | `.../billing.service.ts` + `cancellations.schema.ts` |
| Simulación de demora | `apps/provisioning-service/src/provisioning/provisioning.service.ts` |
| Email | `apps/notification-service/src/notification/notification.service.ts` |
| Auditoría / replay | `apps/crm-analytics-service/src/crm/crm.consumer.ts` |
| Replay de fidelización | `apps/loyalty-service/src/loyalty/loyalty.consumer.ts` |
| Creación de topics | `docker/create-topics.sh` |
| Infraestructura | `docker-compose.yml` |

---

## 6. Errores comunes que conviene evitar en el oral

- Decir que Kafka “borra el mensaje cuando se consume”. **No**: el log se retiene
  7 días; lo que avanza es el offset del grupo.
- Decir que la clave garantiza orden **global**. Garantiza orden **por clave /
  partición**.
- Decir que la compensación es un “rollback”. Es una **acción nueva** que revierte
  el efecto de negocio.
- Decir que el 202 significa “terminado”. Significa **aceptado y en proceso**.
- Confundir `eventId` (id del mensaje) con `correlationId` (id de la Saga).
- Decir que la DLQ recibe los fallos de negocio. Recibe solo **fallos técnicos**
  después de 3 reintentos.
- Olvidar que `activation-api` también es consumidor (de `billing.events` y
  `provisioning.events`).
- Decir que hay exactly-once. Hay **at-least-once + idempotencia**.

---

## 7. Comandos útiles durante el oral

```bash
# Estado de los contenedores
docker compose ps

# Logs de un servicio
docker compose logs -f activation-api

# Ver grupos y lag
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group activation-api-group

# Ver topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Leer mensajes de un topic desde el inicio
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic activation.events --from-beginning

# Escalar billing
docker compose up -d --scale billing-service=2

# Levantar loyalty (replay)
docker compose --profile loyalty up -d loyalty-service

# Demo limpia
docker compose down -v
docker compose up --build -d
```

> En **Git Bash** de Windows, los comandos con rutas absolutas dentro del
> contenedor (por ejemplo `/opt/kafka/bin/...`) pueden necesitar
> `MSYS_NO_PATHCONV=1` adelante. En PowerShell no hace falta.

/**
 * SISTEMAS DISTRIBUIDOS - 2026
 * Kafka Toolkit (libs/kafka-toolkit)
 *
 * Utilidades reutilizables por todos los servicios para cumplir los
 * requerimientos no funcionales de la POC:
 *
 *   1. createEnvelope   -> sobre común (RNF-06 / RNF-07)
 *   2. KafkaPublisher   -> publicar con clave = customerId (RNF-02)
 *   3. runIdempotent    -> idempotencia con MongoDB (RNF-03)
 *   4. withRetryAndDlq  -> reintentos 1s/2s/4s y DLQ (RNF-04)
 */

import { EventEnvelope, EventType } from '@poc/contracts';
import { Kafka, Producer } from 'kafkajs';
import { randomUUID } from 'crypto';

// ============================================================
// 1. SOBRE COMÚN DE EVENTOS (ENVELOPE)
// ============================================================
export function createEnvelope<T>(params: {
  eventType: EventType | string;
  correlationId: string;
  customerId: string;
  source: string;
  payload: T;
  version?: number;
}): EventEnvelope<T> {
  return {
    eventId: randomUUID(),
    eventType: params.eventType,
    version: params.version ?? 1,
    occurredAt: new Date().toISOString(),
    correlationId: params.correlationId,
    customerId: params.customerId,
    source: params.source,
    payload: params.payload,
  };
}

// ============================================================
// 2. PUBLICADOR KAFKA CON CLAVE EXPLÍCITA
//
// Usamos kafkajs directamente (en lugar de ClientKafka.emit) porque
// necesitamos enviar customerId como KEY del mensaje. La key es la que
// decide la partición y garantiza el orden por cliente (RNF-02).
// ClientKafka.emit usa el nombre del evento como key, lo que rompería
// ese requerimiento.
// ============================================================
export interface PublishOptions {
  key: string;
  value: unknown;
  headers?: Record<string, string>;
}

export class KafkaPublisher {
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private connected = false;

  constructor(brokers: string[], clientId: string) {
    this.kafka = new Kafka({ brokers, clientId });
    this.producer = this.kafka.producer();
  }

  async connect(): Promise<void> {
    if (!this.connected) {
      await this.producer.connect();
      this.connected = true;
    }
  }

  async publish(topic: string, options: PublishOptions): Promise<void> {
    await this.connect();
    await this.producer.send({
      topic,
      messages: [
        {
          key: options.key ?? 'sin-clave',
          value: JSON.stringify(options.value),
          headers: options.headers,
        },
      ],
    });
  }

  async disconnect(): Promise<void> {
    if (this.connected) {
      await this.producer.disconnect();
      this.connected = false;
    }
  }
}

// ============================================================
// 3. IDEMPOTENCIA (RNF-03)
//
// Kafka entrega "al menos una vez": un mensaje puede llegar repetido.
// Insertamos el eventId en `processed_events` (índice único). Si ya
// existe, el evento se descarta sin repetir sus efectos.
// ============================================================
export async function checkIdempotency(
  collection: any,
  eventId: string,
  serviceName: string,
): Promise<{ isDuplicate: boolean }> {
  try {
    await collection.insertOne({
      _id: eventId,
      eventId,
      serviceName,
      processedAt: new Date(),
    });
    return { isDuplicate: false };
  } catch (error: any) {
    // 11000 = duplicate key en MongoDB
    if (error.code === 11000 || String(error.message).includes('duplicate key')) {
      console.warn(
        `[IDEMPOTENCIA] [${serviceName}] Evento duplicado ${eventId}: se descarta sin repetir efectos.`,
      );
      return { isDuplicate: true };
    }
    throw error;
  }
}

/**
 * Ejecuta un efecto de negocio de forma idempotente.
 * Si el efecto falla, borra la marca para que el reintento vuelva a ejecutarlo.
 */
export async function runIdempotent<T>(
  collection: any,
  eventId: string,
  serviceName: string,
  effect: () => Promise<T>,
): Promise<{ skipped: boolean; result?: T }> {
  const { isDuplicate } = await checkIdempotency(collection, eventId, serviceName);
  if (isDuplicate) {
    return { skipped: true };
  }

  try {
    const result = await effect();
    return { skipped: false, result };
  } catch (error) {
    // Deshacemos la marca para que el próximo intento sí procese el evento.
    await collection.deleteOne({ _id: eventId });
    throw error;
  }
}

// ============================================================
// 4. REINTENTOS Y DLQ (RNF-04)
//
// Error técnico (base caída, JSON inválido, etc.):
//   intento 1 -> espera 1s
//   intento 2 -> espera 2s
//   intento 3 -> espera 4s
//   si sigue fallando -> publica en <topic>.dlq y continúa.
//
// Los fallos de NEGOCIO simulados (simulateFailure) NO pasan por acá:
// se publican como BillingFailed / ProvisioningFailed.
// ============================================================
export type DlqPublisher = (
  dlqTopic: string,
  envelope: EventEnvelope<any>,
  error: Error,
) => Promise<void>;

export async function withRetryAndDlq(params: {
  operation: () => Promise<void>;
  envelope: EventEnvelope<any>;
  sourceTopic: string;
  serviceName: string;
  publishToDlq: DlqPublisher;
  maxRetries?: number;
}): Promise<void> {
  const { operation, envelope, sourceTopic, serviceName, publishToDlq, maxRetries = 3 } = params;
  const delays = [1000, 2000, 4000];

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await operation();
      return;
    } catch (error: any) {
      console.error(
        `[REINTENTO] [${serviceName}] Intento ${attempt}/${maxRetries} falló ` +
          `(eventId=${envelope?.eventId ?? 'desconocido'}): ${error.message}`,
      );

      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, delays[attempt - 1]));
        continue;
      }

      const dlqTopic = `${sourceTopic}.dlq`;
      console.error(
        `[DLQ] [${serviceName}] El evento superó ${maxRetries} intentos. Se publica en ${dlqTopic}.`,
      );

      try {
        await publishToDlq(dlqTopic, envelope, error);
      } catch (dlqError: any) {
        console.error(`[DLQ] [${serviceName}] No se pudo publicar en la DLQ: ${dlqError.message}`);
      }
    }
  }
}

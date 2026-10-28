import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { KafkaPublisher } from '@poc/kafka-toolkit';
import { EventEnvelope } from '@poc/contracts';
import { BROKERS, CLIENT_ID } from '../kafka/kafka.config';

/**
 * Único punto de salida hacia Kafka.
 *
 * Se usa kafkajs directo para poder enviar `customerId` como KEY.
 * La KEY define la partición y garantiza el orden por cliente (RNF-02).
 */
@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private readonly publisher = new KafkaPublisher(BROKERS, `${CLIENT_ID}-producer`);

  async onModuleInit(): Promise<void> {
    await this.publisher.connect();
    this.logger.log(`Productor Kafka conectado a ${BROKERS.join(', ')}`);
  }

  async onModuleDestroy(): Promise<void> {
    await this.publisher.disconnect();
  }

  /** Publica un sobre de evento usando customerId como clave. */
  async publish(topic: string, envelope: EventEnvelope<any>): Promise<void> {
    await this.publisher.publish(topic, {
      key: envelope.customerId,
      value: envelope,
      headers: { 'x-event-type': envelope.eventType },
    });
    this.logger.log(`Evento ${envelope.eventType} publicado en ${topic}`);
  }

  /** Publica un payload arbitrario (se usa para la DLQ). */
  async publishRaw(
    topic: string,
    key: string,
    value: unknown,
    headers: Record<string, string> = {},
  ): Promise<void> {
    await this.publisher.publish(topic, { key, value, headers });
  }
}

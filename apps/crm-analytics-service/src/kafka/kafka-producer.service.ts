import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { KafkaPublisher } from '@poc/kafka-toolkit';
import { BROKERS, CLIENT_ID } from './kafka.config';

/**
 * CRM no publica eventos de negocio. Solo usa el productor para derivar
 * mensajes que fallaron 3 veces a la DLQ (RNF-04).
 */
@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private readonly publisher = new KafkaPublisher(BROKERS, `${CLIENT_ID}-producer`);

  async onModuleInit(): Promise<void> {
    await this.publisher.connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.publisher.disconnect();
  }

  async publishRaw(
    topic: string,
    key: string,
    value: unknown,
    headers: Record<string, string> = {},
  ): Promise<void> {
    await this.publisher.publish(topic, { key, value, headers });
  }
}

import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { KafkaPublisher } from '@poc/kafka-toolkit';
import { BROKERS, CLIENT_ID } from './kafka.config';

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

  /** Solo se usa para publicar mensajes fallidos en la DLQ. */
  async publishRaw(
    topic: string,
    key: string,
    value: unknown,
    headers: Record<string, string> = {},
  ): Promise<void> {
    await this.publisher.publish(topic, { key, value, headers });
  }
}

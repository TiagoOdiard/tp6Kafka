import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { runIdempotent, withRetryAndDlq } from '@poc/kafka-toolkit';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ProvisioningService } from './provisioning.service';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { ProcessedEvent } from '../schemas/processed-event.schema';

@Controller()
export class ProvisioningConsumer {
  private readonly logger = new Logger(ProvisioningConsumer.name);

  constructor(
    private readonly provisioningService: ProvisioningService,
    private readonly producer: KafkaProducerService,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<any>,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    try {
      await withRetryAndDlq({
        operation: async () => {
          if (!envelope?.eventId || !envelope?.correlationId) {
            throw new Error('Evento inválido: faltan campos eventId/correlationId');
          }
          await runIdempotent(
            this.processedModel.collection,
            envelope.eventId,
            'provisioning-service',
            () => this.provisioningService.handleActivationRequested(envelope),
          );
        },
        envelope,
        sourceTopic: TOPICS.ACTIVATION_REQUESTED,
        serviceName: 'provisioning-service',
        publishToDlq: (dlqTopic, event, error) => this.publishToDlq(dlqTopic, event, error),
      });
    } finally {
      await this.commitOffset(context);
    }
  }

  private async publishToDlq(
    dlqTopic: string,
    envelope: EventEnvelope<any>,
    error: Error,
  ): Promise<void> {
    await this.producer.publishRaw(
      dlqTopic,
      envelope?.customerId ?? 'sin-clave',
      {
        originalEnvelope: envelope,
        error: {
          message: error.message,
          service: 'provisioning-service',
          failedAt: new Date().toISOString(),
        },
      },
      {
        'x-error': error.message.slice(0, 250),
        'x-original-event-id': envelope?.eventId ?? 'desconocido',
      },
    );
  }

  private async commitOffset(context: KafkaContext): Promise<void> {
    try {
      const topic = context.getTopic();
      const partition = context.getPartition();
      const nextOffset = (Number(context.getMessage().offset) + 1).toString();
      await context.getConsumer().commitOffsets([{ topic, partition, offset: nextOffset }]);
    } catch (error: any) {
      this.logger.error(`No se pudo confirmar el offset: ${error.message}`);
    }
  }
}

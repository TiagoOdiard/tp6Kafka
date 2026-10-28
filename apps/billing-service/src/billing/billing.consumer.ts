import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { runIdempotent, withRetryAndDlq } from '@poc/kafka-toolkit';
import { EVENT_TYPES, EventEnvelope, TOPICS } from '@poc/contracts';
import { BillingService } from './billing.service';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { ProcessedEvent } from '../schemas/processed-event.schema';

@Controller()
export class BillingConsumer {
  private readonly logger = new Logger(BillingConsumer.name);

  constructor(
    private readonly billingService: BillingService,
    private readonly producer: KafkaProducerService,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<any>,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.handle(TOPICS.ACTIVATION_REQUESTED, envelope, context, () =>
      this.billingService.handleActivationRequested(envelope),
    );
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivationEvent(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    if (envelope?.eventType !== EVENT_TYPES.ACTIVATION_FAILED) {
      // ActivationCompleted no le interesa a billing.
      await this.commitOffset(context);
      return;
    }

    await this.handle(TOPICS.ACTIVATION_EVENTS, envelope, context, () =>
      this.billingService.handleActivationFailed(envelope),
    );
  }

  private async handle(
    topic: string,
    envelope: EventEnvelope<any>,
    context: KafkaContext,
    effect: () => Promise<void>,
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
            'billing-service',
            effect,
          );
        },
        envelope,
        sourceTopic: topic,
        serviceName: 'billing-service',
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
          service: 'billing-service',
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

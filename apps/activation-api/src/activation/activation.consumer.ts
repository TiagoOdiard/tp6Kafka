import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { runIdempotent, withRetryAndDlq } from '@poc/kafka-toolkit';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { ActivationService } from './activation.service';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { ProcessedEvent } from '../schemas/processed-event.schema';

/**
 * Consumidor del agregador.
 *
 * Escucha los resultados de billing y provisioning, aplica idempotencia,
 * reintentos y DLQ, y recién al final confirma el offset.
 */
@Controller()
export class ActivationConsumer {
  private readonly logger = new Logger(ActivationConsumer.name);

  constructor(
    private readonly activationService: ActivationService,
    private readonly producer: KafkaProducerService,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<any>,
  ) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  async onBillingEvent(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.handle(TOPICS.BILLING_EVENTS, envelope, context);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async onProvisioningEvent(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.handle(TOPICS.PROVISIONING_EVENTS, envelope, context);
  }

  private async handle(
    topic: string,
    envelope: EventEnvelope<any>,
    context: KafkaContext,
  ): Promise<void> {
    try {
      await withRetryAndDlq({
        operation: () => this.process(topic, envelope),
        envelope,
        sourceTopic: topic,
        serviceName: 'activation-api',
        publishToDlq: (dlqTopic, event, error) => this.publishToDlq(dlqTopic, event, error),
      });
    } finally {
      // RNF-05: commit manual DESPUÉS de procesar (o de mandar a la DLQ).
      await this.commitOffset(context);
    }
  }

  private async process(topic: string, envelope: EventEnvelope<any>): Promise<void> {
    if (!envelope?.eventId || !envelope?.correlationId) {
      throw new Error('Evento inválido: faltan campos eventId/correlationId');
    }

    await runIdempotent(this.processedModel.collection, envelope.eventId, 'activation-api', () =>
      this.activationService.handleStepEvent(envelope),
    );

    this.logger.log(
      `[${topic}] ${envelope.eventType} procesado (activación ${envelope.correlationId})`,
    );
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
          service: 'activation-api',
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

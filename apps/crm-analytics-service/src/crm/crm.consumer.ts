import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { runIdempotent, withRetryAndDlq } from '@poc/kafka-toolkit';
import { EventEnvelope, TOPICS } from '@poc/contracts';
import { EventLog } from '../schemas/event-log.schema';
import { ProcessedEvent } from '../schemas/processed-event.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

/**
 * Consumidor pasivo (RF-10).
 *
 * Escucha los 4 topics de negocio, guarda una copia de cada evento y NO
 * publica nada. Demuestra que se puede sumar un consumidor nuevo sin tocar
 * a los demás servicios.
 */
@Controller()
export class CrmConsumer {
  private readonly logger = new Logger(CrmConsumer.name);

  constructor(
    @InjectModel(EventLog.name) private readonly eventLog: Model<any>,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<any>,
    private readonly producer: KafkaProducerService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.handle(TOPICS.ACTIVATION_REQUESTED, envelope, context);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async onBilling(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.handle(TOPICS.BILLING_EVENTS, envelope, context);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async onProvisioning(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.handle(TOPICS.PROVISIONING_EVENTS, envelope, context);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivation(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
  ): Promise<void> {
    await this.handle(TOPICS.ACTIVATION_EVENTS, envelope, context);
  }

  private async handle(
    topic: string,
    envelope: EventEnvelope<any>,
    context: KafkaContext,
  ): Promise<void> {
    try {
      await withRetryAndDlq({
        operation: async () => {
          if (!envelope?.eventId) {
            throw new Error('Evento inválido: falta eventId');
          }

          await runIdempotent(
            this.processedModel.collection,
            envelope.eventId,
            'crm-analytics-service',
            async () => {
              await this.eventLog.updateOne(
                { eventId: envelope.eventId },
                {
                  $setOnInsert: {
                    eventId: envelope.eventId,
                    eventType: envelope.eventType,
                    correlationId: envelope.correlationId,
                    customerId: envelope.customerId,
                    source: envelope.source,
                    topic,
                    partition: context.getPartition(),
                    offset: context.getMessage().offset,
                    occurredAt: new Date(envelope.occurredAt),
                    receivedAt: new Date(),
                    payload: envelope.payload,
                  },
                },
                { upsert: true },
              );
            },
          );
        },
        envelope,
        sourceTopic: topic,
        serviceName: 'crm-analytics-service',
        publishToDlq: async (dlqTopic, event, error) => {
          await this.producer.publishRaw(
            dlqTopic,
            event?.customerId ?? 'sin-clave',
            {
              originalEnvelope: event,
              error: {
                message: error.message,
                service: 'crm-analytics-service',
                failedAt: new Date().toISOString(),
              },
            },
            {
              'x-error': error.message.slice(0, 250),
              'x-original-event-id': event?.eventId ?? 'desconocido',
            },
          );
        },
      });
    } finally {
      await this.commitOffset(context);
    }
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

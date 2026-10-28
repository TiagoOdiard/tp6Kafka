import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { runIdempotent, withRetryAndDlq } from '@poc/kafka-toolkit';
import { EVENT_TYPES, EventEnvelope, TOPICS } from '@poc/contracts';
import { LoyaltyAccount } from '../schemas/loyalty-account.schema';
import { ProcessedEvent } from '../schemas/processed-event.schema';

/**
 * Servicio "nuevo" del escenario 5.
 *
 * No existía cuando se crearon las activaciones. Al arrancar con un
 * consumer group nuevo y fromBeginning=true, Kafka le entrega todo el
 * historial: eso es el replay del log (event sourcing).
 */
@Controller()
export class LoyaltyConsumer {
  private readonly logger = new Logger(LoyaltyConsumer.name);

  constructor(
    @InjectModel(LoyaltyAccount.name) private readonly accounts: Model<any>,
    @InjectModel(ProcessedEvent.name) private readonly processedModel: Model<any>,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async onActivationEvent(
    @Payload() envelope: EventEnvelope<any>,
    @Ctx() context: KafkaContext,
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
            'loyalty-service',
            async () => {
              if (envelope.eventType !== EVENT_TYPES.ACTIVATION_COMPLETED) {
                return;
              }

              await this.accounts.updateOne(
                { customerId: envelope.customerId },
                {
                  $inc: { points: 10, activations: 1 },
                  $set: { updatedAt: new Date(), customerId: envelope.customerId },
                },
                { upsert: true },
              );

              this.logger.log(
                `+10 puntos para ${envelope.customerId} por ${envelope.correlationId} (replay del log)`,
              );
            },
          );
        },
        envelope,
        sourceTopic: TOPICS.ACTIVATION_EVENTS,
        serviceName: 'loyalty-service',
        publishToDlq: async (dlqTopic, _event, error) => {
          this.logger.error(`No se pudo procesar el evento. DLQ ${dlqTopic}: ${error.message}`);
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

import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { createEnvelope } from '@poc/kafka-toolkit';
import { EVENT_TYPES, EventEnvelope, TOPICS } from '@poc/contracts';
import { ProvisioningOrder } from '../schemas/provisioning-order.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

/**
 * Aprovisionamiento técnico simulado.
 *
 * - Éxito: espera 1 a 3 segundos y publica ProvisioningCompleted.
 * - Fallo simulado: publica ProvisioningFailed (fallo de negocio, no DLQ).
 */
@Injectable()
export class ProvisioningService {
  private readonly logger = new Logger(ProvisioningService.name);

  constructor(
    @InjectModel(ProvisioningOrder.name) private readonly orders: Model<any>,
    private readonly producer: KafkaProducerService,
  ) {}

  async handleActivationRequested(envelope: EventEnvelope<any>): Promise<void> {
    const activationId = envelope.correlationId;
    const planId = envelope.payload?.planId ?? 'PLAN DIGITAL';
    const simulateFailure = envelope.payload?.simulateFailure ?? 'none';

    const delayMs = 1000 + Math.floor(Math.random() * 2000); // 1 a 3 segundos
    this.logger.log(`Aprovisionando ${activationId} (demora simulada ${delayMs} ms)`);
    await this.sleep(delayMs);

    if (simulateFailure === 'provisioning') {
      this.logger.warn(`Fallo de aprovisionamiento simulado para ${activationId}`);
      const failedEvent = createEnvelope({
        eventType: EVENT_TYPES.PROVISIONING_FAILED,
        correlationId: activationId,
        customerId: envelope.customerId,
        source: 'provisioning-service',
        payload: { reason: 'Sin capacidad en la red (fallo simulado)' },
      });
      await this.producer.publish(TOPICS.PROVISIONING_EVENTS, failedEvent);
      return;
    }

    const existing = await this.orders.findOne({ activationId });
    if (!existing) {
      await this.orders.create({
        activationId,
        orderId: `ord-${activationId.replace('act-', '')}`,
        customerId: envelope.customerId,
        planId,
        status: 'COMPLETED',
        createdAt: new Date(),
      });
    }

    const completedEvent = createEnvelope({
      eventType: EVENT_TYPES.PROVISIONING_COMPLETED,
      correlationId: activationId,
      customerId: envelope.customerId,
      source: 'provisioning-service',
      payload: {
        provisioningId: `prov-${activationId.replace('act-', '')}`,
        serviceDetails: `Servicio ${planId} habilitado correctamente`,
      },
    });

    await this.producer.publish(TOPICS.PROVISIONING_EVENTS, completedEvent);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

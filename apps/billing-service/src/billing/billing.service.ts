import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { createEnvelope } from '@poc/kafka-toolkit';
import { EVENT_TYPES, EventEnvelope, TOPICS } from '@poc/contracts';
import { BillingAccount } from '../schemas/billing-account.schema';
import { Cancellation } from '../schemas/cancellation.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

/**
 * Servicio de facturación simulada.
 *
 * Flujo normal:     ActivationRequested -> crea cuenta -> BillingAccountCreated
 * Fallo simulado:   ActivationRequested -> BillingFailed (no crea cuenta)
 * Compensación:     ActivationFailed     -> anula la cuenta -> BillingAccountCancelled
 *
 * IMPORTANTE: los fallos simulados son fallos de NEGOCIO, no técnicos.
 * Se publican como eventos y NO van a la DLQ.
 */
@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name) private readonly accounts: Model<any>,
    @InjectModel(Cancellation.name) private readonly cancellations: Model<any>,
    private readonly producer: KafkaProducerService,
  ) {}

  async handleActivationRequested(envelope: EventEnvelope<any>): Promise<void> {
    const activationId = envelope.correlationId;
    const planId = envelope.payload?.planId ?? 'PLAN DIGITAL';
    const simulateFailure = envelope.payload?.simulateFailure ?? 'none';

    if (simulateFailure === 'billing') {
      this.logger.warn(`Fallo de facturación simulado para ${activationId}`);
      const failedEvent = createEnvelope({
        eventType: EVENT_TYPES.BILLING_FAILED,
        correlationId: activationId,
        customerId: envelope.customerId,
        source: 'billing-service',
        payload: { reason: 'Tarjeta rechazada (fallo simulado)' },
      });
      await this.producer.publish(TOPICS.BILLING_EVENTS, failedEvent);
      return;
    }

    let account = await this.accounts.findOne({ activationId });

    if (!account) {
      account = await this.accounts.create({
        activationId,
        accountId: `acc-${activationId.replace('act-', '')}`,
        customerId: envelope.customerId,
        planId,
        amount: this.planAmount(planId),
        status: 'CREATED',
        createdAt: new Date(),
      });
      this.logger.log(`Cuenta ${account.accountId} creada para ${activationId}`);
    }

    if (account.status === 'CREATED') {
      const createdEvent = createEnvelope({
        eventType: EVENT_TYPES.BILLING_ACCOUNT_CREATED,
        correlationId: activationId,
        customerId: envelope.customerId,
        source: 'billing-service',
        payload: {
          accountId: account.accountId,
          amount: account.amount,
          planId,
        },
      });
      await this.producer.publish(TOPICS.BILLING_EVENTS, createdEvent);
    }

    // Caso borde: el fallo llegó antes de que existiera la cuenta.
    const cancellation: any = await this.cancellations.findById(activationId).lean();
    if (cancellation) {
      await this.cancelAccount(activationId, cancellation.reason);
    }
  }

  async handleActivationFailed(envelope: EventEnvelope<any>): Promise<void> {
    const activationId = envelope.correlationId;
    const reason = envelope.payload?.reason ?? 'Activación fallida';

    // Guardamos la intención de cancelar (aunque la cuenta no exista todavía).
    await this.cancellations.updateOne(
      { _id: activationId },
      { $setOnInsert: { reason, receivedAt: new Date() } },
      { upsert: true },
    );

    await this.cancelAccount(activationId, reason);
  }

  /**
   * Compensación idempotente: solo publica el evento si la cuenta
   * efectivamente pasó de CREATED a CANCELLED.
   */
  private async cancelAccount(activationId: string, reason: string): Promise<void> {
    const cancelled = await this.accounts.findOneAndUpdate(
      { activationId, status: 'CREATED' },
      { $set: { status: 'CANCELLED', cancelReason: reason, cancelledAt: new Date() } },
      { new: true },
    );

    if (!cancelled) {
      return;
    }

    this.logger.warn(`Cuenta ${cancelled.accountId} cancelada (compensación): ${reason}`);

    const cancelledEvent = createEnvelope({
      eventType: EVENT_TYPES.BILLING_ACCOUNT_CANCELLED,
      correlationId: activationId,
      customerId: cancelled.customerId,
      source: 'billing-service',
      payload: { accountId: cancelled.accountId, reason },
    });

    await this.producer.publish(TOPICS.BILLING_EVENTS, cancelledEvent);
  }

  private planAmount(planId: string): number {
    const prices: Record<string, number> = {
      'PLAN BASICO': 19.99,
      'PLAN DIGITAL': 29.99,
      'PLAN PREMIUM': 49.99,
    };
    return prices[planId.toUpperCase()] ?? 29.99;
  }
}

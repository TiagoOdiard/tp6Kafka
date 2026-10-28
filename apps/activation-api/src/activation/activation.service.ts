import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { randomUUID } from 'crypto';
import { createEnvelope } from '@poc/kafka-toolkit';
import { EVENT_TYPES, EventEnvelope, TOPICS } from '@poc/contracts';
import { Activation, ActivationDocument } from '../schemas/activation.schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';
import { ActivationGateway } from '../gateway/activation.gateway';
import { CreateActivationDto } from '../dto/create-activation.dto';
import { ActivationEventWsPayload } from '../kafka/kafka.config';

/**
 * Corazón del agregador de la Saga (coreografía).
 *
 * billing y provisioning trabajan en paralelo. Este servicio NO les da
 * órdenes: solo escucha sus resultados y decide el estado final.
 */
@Injectable()
export class ActivationService {
  private readonly logger = new Logger(ActivationService.name);

  constructor(
    @InjectModel(Activation.name)
    private readonly activationModel: Model<ActivationDocument>,
    private readonly producer: KafkaProducerService,
    private readonly gateway: ActivationGateway,
  ) {}

  // ---------------------------------------------------------
  // RF-01 / RF-02 / RF-03: crear activación y publicar el evento
  // ---------------------------------------------------------
  async create(dto: CreateActivationDto): Promise<{ activationId: string; status: string }> {
    const now = new Date();
    const activationId = `act-${randomUUID().slice(0, 8)}`;
    const simulateFailure = dto.simulateFailure ?? 'none';

    const envelope = createEnvelope({
      eventType: EVENT_TYPES.ACTIVATION_REQUESTED,
      correlationId: activationId,
      customerId: dto.customerId,
      source: 'activation-api',
      payload: { planId: dto.planId, channel: 'web', simulateFailure },
    });

    await this.activationModel.create({
      _id: activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      simulateFailure,
      status: 'PENDING',
      steps: {},
      history: [this.toHistoryEntry(envelope)],
      createdAt: now,
      updatedAt: now,
    });

    // Se publica después de guardar. Si el proceso muriera entre ambos pasos
    // el evento se perdería: eso lo resuelve el Outbox (fase 4, fuera del alcance).
    await this.producer.publish(TOPICS.ACTIVATION_REQUESTED, envelope);
    await this.emitWs(activationId, envelope);

    this.logger.log(`Activación ${activationId} creada para ${dto.customerId}`);
    return { activationId, status: 'PENDING' };
  }

  // ---------------------------------------------------------
  // RF-09: consultas
  // ---------------------------------------------------------
  async findAll(): Promise<any[]> {
    return this.activationModel.find().sort({ createdAt: -1 }).limit(20).lean();
  }

  async findOne(id: string): Promise<any> {
    const activation = await this.activationModel.findById(id).lean();
    if (!activation) {
      throw new NotFoundException(`No existe la activación ${id}`);
    }
    return activation;
  }

  // ---------------------------------------------------------
  // RF-04 / RF-05 / RF-06: agregación de resultados
  // ---------------------------------------------------------
  async handleStepEvent(envelope: EventEnvelope<any>): Promise<void> {
    const activationId = envelope.correlationId;
    const activation = await this.activationModel.findById(activationId).lean();

    if (!activation) {
      this.logger.warn(
        `Evento ${envelope.eventType} de activación desconocida ${activationId}: se ignora.`,
      );
      return;
    }

    const step = this.stepForEvent(envelope.eventType);
    // La cancelación es la compensación de billing: no modifica ningún paso,
    // pero sí debe quedar registrada en la timeline.
    const isCompensation = envelope.eventType === EVENT_TYPES.BILLING_ACCOUNT_CANCELLED;

    if (!step && !isCompensation) {
      this.logger.warn(`Evento ${envelope.eventType} no aplica a la Saga: se ignora.`);
      return;
    }

    // 1) Historial (idempotente: no duplica si el eventId ya está)
    await this.activationModel.updateOne(
      { _id: activationId, 'history.eventId': { $ne: envelope.eventId } },
      {
        $push: { history: this.toHistoryEntry(envelope) },
        $set: { updatedAt: new Date() },
      },
    );

    // Si es una compensación, solo se registra en el historial.
    if (!step) {
      await this.emitWs(activationId, envelope);
      return;
    }

    // 2) Resultado parcial del paso (se guarda el primero que llega)
    const failed = this.isFailureEvent(envelope.eventType);
    const stepResult = {
      status: failed ? ('FAILED' as const) : ('OK' as const),
      at: new Date(envelope.occurredAt),
      reason: failed ? envelope.payload?.reason ?? 'Fallo simulado' : undefined,
    };

    await this.activationModel.updateOne(
      { _id: activationId, [`steps.${step}`]: { $exists: false } },
      { $set: { [`steps.${step}`]: stepResult, updatedAt: new Date() } },
    );

    const current = await this.activationModel.findById(activationId).lean();
    if (!current) {
      return;
    }

    // 3) Si la Saga ya terminó, solo registramos el evento posterior.
    if (current.status === 'ACTIVE' || current.status === 'FAILED') {
      await this.emitWs(activationId, envelope);
      return;
    }

    // 4) Primer fallo -> FAILED (RF-06)
    if (
      current.steps?.billing?.status === 'FAILED' ||
      current.steps?.provisioning?.status === 'FAILED'
    ) {
      // Primero mostramos el evento que provocó el fallo y después el cierre.
      await this.emitWs(activationId, envelope);
      await this.finishFailed(activationId, current);
      return;
    }

    // 5) Ambos OK -> ACTIVE (RF-05)
    if (
      current.steps?.billing?.status === 'OK' &&
      current.steps?.provisioning?.status === 'OK'
    ) {
      await this.emitWs(activationId, envelope);
      await this.finishActive(activationId, current);
      return;
    }

    // 6) Quedó un solo resultado: la Saga sigue en curso.
    if (current.status === 'PENDING') {
      await this.activationModel.updateOne(
        { _id: activationId, status: 'PENDING' },
        { $set: { status: 'IN_PROGRESS', updatedAt: new Date() } },
      );
    }

    await this.emitWs(activationId, envelope);
  }

  // ---------------------------------------------------------
  // Transiciones finales (protegidas con update condicional para
  // que dos eventos concurrentes no publiquen dos veces el cierre).
  // ---------------------------------------------------------
  private async finishActive(activationId: string, activation: Activation): Promise<void> {
    const updated = await this.activationModel.findOneAndUpdate(
      { _id: activationId, status: { $nin: ['ACTIVE', 'FAILED'] } },
      { $set: { status: 'ACTIVE', updatedAt: new Date() } },
      { new: true },
    );

    if (!updated) {
      return;
    }

    const envelope = createEnvelope({
      eventType: EVENT_TYPES.ACTIVATION_COMPLETED,
      correlationId: activationId,
      customerId: activation.customerId,
      source: 'activation-api',
      payload: { planId: activation.planId, completedAt: new Date().toISOString() },
    });

    await this.activationModel.updateOne(
      { _id: activationId },
      { $push: { history: this.toHistoryEntry(envelope) }, $set: { updatedAt: new Date() } },
    );
    await this.producer.publish(TOPICS.ACTIVATION_EVENTS, envelope);
    await this.emitWs(activationId, envelope);
  }

  private async finishFailed(activationId: string, activation: Activation): Promise<void> {
    const updated = await this.activationModel.findOneAndUpdate(
      { _id: activationId, status: { $nin: ['ACTIVE', 'FAILED'] } },
      { $set: { status: 'FAILED', updatedAt: new Date() } },
      { new: true },
    );

    if (!updated) {
      return;
    }

    const failedStep = activation.steps?.billing?.status === 'FAILED' ? 'billing' : 'provisioning';
    const reason =
      (failedStep === 'billing'
        ? activation.steps?.billing?.reason
        : activation.steps?.provisioning?.reason) ?? 'Fallo simulado';

    const envelope = createEnvelope({
      eventType: EVENT_TYPES.ACTIVATION_FAILED,
      correlationId: activationId,
      customerId: activation.customerId,
      source: 'activation-api',
      payload: {
        planId: activation.planId,
        reason,
        failedStep,
      },
    });

    await this.activationModel.updateOne(
      { _id: activationId },
      { $push: { history: this.toHistoryEntry(envelope) }, $set: { updatedAt: new Date() } },
    );
    await this.producer.publish(TOPICS.ACTIVATION_EVENTS, envelope);
    await this.emitWs(activationId, envelope);
  }

  // ---------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------
  private stepForEvent(eventType: string): 'billing' | 'provisioning' | null {
    if (
      eventType === EVENT_TYPES.BILLING_ACCOUNT_CREATED ||
      eventType === EVENT_TYPES.BILLING_FAILED
    ) {
      return 'billing';
    }
    if (
      eventType === EVENT_TYPES.PROVISIONING_COMPLETED ||
      eventType === EVENT_TYPES.PROVISIONING_FAILED
    ) {
      return 'provisioning';
    }
    return null;
  }

  private isFailureEvent(eventType: string): boolean {
    return (
      eventType === EVENT_TYPES.BILLING_FAILED ||
      eventType === EVENT_TYPES.PROVISIONING_FAILED
    );
  }

  private toHistoryEntry(envelope: EventEnvelope<any>) {
    return {
      eventId: envelope.eventId,
      eventType: envelope.eventType,
      source: envelope.source,
      at: new Date(envelope.occurredAt),
      payload: envelope.payload,
    };
  }

  private async emitWs(activationId: string, envelope: EventEnvelope<any>): Promise<void> {
    const activation = await this.activationModel.findById(activationId).lean();
    if (!activation) {
      return;
    }

    const payload: ActivationEventWsPayload = {
      activationId,
      status: activation.status,
      customerId: activation.customerId,
      planId: activation.planId,
      event: {
        eventId: envelope.eventId,
        eventType: envelope.eventType,
        source: envelope.source,
        occurredAt: envelope.occurredAt,
        payload: envelope.payload,
      },
      steps: activation.steps,
      receivedAt: new Date().toISOString(),
    };

    this.gateway.emitActivationEvent(payload);
  }
}

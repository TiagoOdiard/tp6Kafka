/**
 * SISTEMAS DISTRIBUIDOS - 2026
 * Contratos de Eventos Compartidos (libs/contracts)
 * 
 * En arquitecturas orientadas a eventos (EDA), esta librería actúa como
 * "Single Source of Truth" (única fuente de verdad) para asegurar que tanto
 * productores como consumidores compartan la misma estructura y nombres.
 */

// ==========================================
// 1. NOMBRES DE TÓPICOS
// ==========================================
export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',

  // Tópicos DLQ (Dead Letter Queue) para mensajes fallidos tras 3 reintentos
  ACTIVATION_REQUESTED_DLQ: 'activation.requested.dlq',
  BILLING_EVENTS_DLQ: 'billing.events.dlq',
  PROVISIONING_EVENTS_DLQ: 'provisioning.events.dlq',
  ACTIVATION_EVENTS_DLQ: 'activation.events.dlq',
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

// ==========================================
// 2. TIPOS DE EVENTOS
// Convención: Verbos en participio pasado (describen hechos consumados)
// ==========================================
export const EVENT_TYPES = {
  ACTIVATION_REQUESTED: 'ActivationRequested',
  BILLING_ACCOUNT_CREATED: 'BillingAccountCreated',
  BILLING_FAILED: 'BillingFailed',
  BILLING_ACCOUNT_CANCELLED: 'BillingAccountCancelled',
  PROVISIONING_COMPLETED: 'ProvisioningCompleted',
  PROVISIONING_FAILED: 'ProvisioningFailed',
  ACTIVATION_COMPLETED: 'ActivationCompleted',
  ACTIVATION_FAILED: 'ActivationFailed',
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

// ==========================================
// 3. EL SOBRE COMÚN (ENVELOPE PATTERN)
// Todos los mensajes viajan envueltos con estos metadatos
// ==========================================
export interface EventEnvelope<T = any> {
  /** Identificador único del evento (UUID v4), base para la idempotencia */
  eventId: string;
  /** Tipo de evento del dominio */
  eventType: EventType | string;
  /** Versión del esquema del evento para permitir evolución hacia atrás */
  version: number;
  /** Marca de tiempo ISO 8601 del momento en que ocurrió el hecho */
  occurredAt: string;
  /** Identificador de correlación (activationId) que agrupa toda la Saga */
  correlationId: string;
  /** Clave de partición en Kafka: garantiza orden de eventos por cliente */
  customerId: string;
  /** Nombre del microservicio que emitió el evento */
  source: string;
  /** Datos de negocio específicos del evento */
  payload: T;
}

// ==========================================
// 4. PAYLOADS ESPECÍFICOS DE NEGOCIO
// ==========================================
export type FailureMode = 'none' | 'billing' | 'provisioning';

export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure: FailureMode;
}

export interface BillingAccountCreatedPayload {
  accountId: string;
  amount: number;
  planId: string;
}

export interface BillingFailedPayload {
  reason: string;
}

export interface BillingAccountCancelledPayload {
  accountId: string;
  reason: string;
}

export interface ProvisioningCompletedPayload {
  provisioningId: string;
  serviceDetails: string;
}

export interface ProvisioningFailedPayload {
  reason: string;
}

export interface ActivationCompletedPayload {
  planId: string;
  completedAt: string;
}

export interface ActivationFailedPayload {
  planId: string;
  reason: string;
  failedStep: 'billing' | 'provisioning';
}

// ==========================================
// 5. ESTADOS DE LA ACTIVACIÓN EN MONGO
// ==========================================
export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

import { KafkaOptions, Transport } from '@nestjs/microservices';
import { EventEnvelope } from '@poc/contracts';

export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

export const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'activation-api';

/**
 * Configuración del servidor Kafka de NestJS (@EventPattern).
 *
 * `autoCommit: false` es intencional: el offset se confirma recién
 * cuando el evento terminó de procesarse (commit manual, RNF-05).
 */
export const kafkaServerOptions: KafkaOptions = {
  transport: Transport.KAFKA,
  options: {
    // Sin sufijo "-server": el groupId queda exactamente como lo definimos.
    postfixId: '',
    client: {
      clientId: CLIENT_ID,
      brokers: BROKERS,
    },
    consumer: {
      groupId: process.env.KAFKA_GROUP_ID ?? 'activation-api-group',
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    },
    run: {
      autoCommit: false,
    },
  },
};

/**
 * Payload que se envía a la UI por WebSocket cada vez que cambia
 * el estado de una activación.
 */
export interface ActivationEventWsPayload {
  activationId: string;
  status: string;
  customerId: string;
  planId: string;
  event: {
    eventId: string;
    eventType: string;
    source: string;
    occurredAt: string;
    payload?: any;
  };
  steps?: any;
  receivedAt: string;
}

export type Envelope = EventEnvelope<any>;

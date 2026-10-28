import { KafkaOptions, Transport } from '@nestjs/microservices';

export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
export const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'crm-analytics-service';

/**
 * CRM es un consumidor pasivo: lee TODOS los topics y no publica nada.
 *
 * `fromBeginning: true` permite reprocesar el historial cuando arranca
 * con un groupId nuevo (escenario de replay).
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
      groupId: process.env.KAFKA_GROUP_ID ?? 'crm-analytics-group',
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    },
    subscribe: {
      fromBeginning: process.env.KAFKA_FROM_BEGINNING !== 'false',
    },
    run: {
      autoCommit: false,
    },
  },
};

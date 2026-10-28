import { KafkaOptions, Transport } from '@nestjs/microservices';

export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
export const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'billing-service';

/**
 * Consumidor Kafka de billing.
 * autoCommit: false -> el offset se confirma después de procesar (RNF-05).
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
      groupId: process.env.KAFKA_GROUP_ID ?? 'billing-service-group',
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    },
    run: {
      autoCommit: false,
    },
  },
};

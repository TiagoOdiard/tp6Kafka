import { KafkaOptions, Transport } from '@nestjs/microservices';

export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
export const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'loyalty-service';

/**
 * Escenario 5: consumidor NUEVO con un groupId propio que lee desde el
 * inicio del log (auto.offset.reset = earliest).
 *
 * Como el grupo no tiene offsets confirmados, Kafka le entrega TODOS los
 * eventos históricos sin afectar a los consumidores que ya existen.
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
      groupId: process.env.KAFKA_GROUP_ID ?? 'loyalty-service-group',
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    },
    subscribe: {
      fromBeginning: true,
    },
    run: {
      autoCommit: false,
    },
  },
};

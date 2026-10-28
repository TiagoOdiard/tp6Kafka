import { KafkaOptions, Transport } from '@nestjs/microservices';

export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
export const CLIENT_ID = process.env.KAFKA_CLIENT_ID ?? 'provisioning-service';

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
      groupId: process.env.KAFKA_GROUP_ID ?? 'provisioning-service-group',
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    },
    run: {
      autoCommit: false,
    },
  },
};

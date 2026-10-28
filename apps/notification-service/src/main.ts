import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';
import { kafkaServerOptions } from './kafka/kafka.config';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createMicroservice(AppModule, kafkaServerOptions);
  await app.listen();
  Logger.log('notification-service escuchando eventos de Kafka', 'notification-service');
}

bootstrap();

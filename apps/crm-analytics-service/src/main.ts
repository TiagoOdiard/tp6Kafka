import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';
import { kafkaServerOptions } from './kafka/kafka.config';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.createMicroservice(AppModule, kafkaServerOptions);
  await app.listen();
  Logger.log(
    'crm-analytics-service escuchando TODOS los topics (consumidor pasivo)',
    'crm-analytics-service',
  );
}

bootstrap();

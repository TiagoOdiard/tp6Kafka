import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger, ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';
import { kafkaServerOptions } from './kafka/kafka.config';

async function bootstrap(): Promise<void> {
  const logger = new Logger('activation-api');
  const app = await NestFactory.create(AppModule);

  app.enableCors({ origin: '*' });
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));

  // Servidor Kafka (@EventPattern) + servidor HTTP en el mismo proceso.
  app.connectMicroservice(kafkaServerOptions);
  await app.startAllMicroservices();
  await app.listen(process.env.PORT ? Number(process.env.PORT) : 3000);

  logger.log('API REST escuchando en http://localhost:3000');
  logger.log('WebSocket (Socket.io) escuchando en el mismo puerto');
}

bootstrap();

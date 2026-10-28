import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ActivationController } from './activation/activation.controller';
import { ActivationService } from './activation/activation.service';
import { ActivationConsumer } from './activation/activation.consumer';
import { ActivationGateway } from './gateway/activation.gateway';
import { KafkaProducerService } from './kafka/kafka-producer.service';
import { Activation, ActivationSchema } from './schemas/activation.schema';
import { ProcessedEvent, ProcessedEventSchema } from './schemas/processed-event.schema';

@Module({
  imports: [
    MongooseModule.forRoot(
      process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation_db',
    ),
    MongooseModule.forFeature([
      { name: Activation.name, schema: ActivationSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [ActivationController, ActivationConsumer],
  providers: [ActivationService, ActivationGateway, KafkaProducerService],
})
export class AppModule {}

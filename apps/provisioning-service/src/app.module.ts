import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ProvisioningConsumer } from './provisioning/provisioning.consumer';
import { ProvisioningService } from './provisioning/provisioning.service';
import { KafkaProducerService } from './kafka/kafka-producer.service';
import {
  ProvisioningOrder,
  ProvisioningOrderSchema,
} from './schemas/provisioning-order.schema';
import { ProcessedEvent, ProcessedEventSchema } from './schemas/processed-event.schema';

@Module({
  imports: [
    MongooseModule.forRoot(
      process.env.MONGO_URI ?? 'mongodb://localhost:27017/provisioning_db',
    ),
    MongooseModule.forFeature([
      { name: ProvisioningOrder.name, schema: ProvisioningOrderSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [ProvisioningConsumer],
  providers: [ProvisioningService, KafkaProducerService],
})
export class AppModule {}

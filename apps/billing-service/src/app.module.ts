import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { BillingConsumer } from './billing/billing.consumer';
import { BillingService } from './billing/billing.service';
import { KafkaProducerService } from './kafka/kafka-producer.service';
import {
  BillingAccount,
  BillingAccountSchema,
} from './schemas/billing-account.schema';
import { Cancellation, CancellationSchema } from './schemas/cancellation.schema';
import { ProcessedEvent, ProcessedEventSchema } from './schemas/processed-event.schema';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI ?? 'mongodb://localhost:27017/billing_db'),
    MongooseModule.forFeature([
      { name: BillingAccount.name, schema: BillingAccountSchema },
      { name: Cancellation.name, schema: CancellationSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [BillingConsumer],
  providers: [BillingService, KafkaProducerService],
})
export class AppModule {}

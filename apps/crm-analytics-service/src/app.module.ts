import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { CrmConsumer } from './crm/crm.consumer';
import { KafkaProducerService } from './kafka/kafka-producer.service';
import { EventLog, EventLogSchema } from './schemas/event-log.schema';
import { ProcessedEvent, ProcessedEventSchema } from './schemas/processed-event.schema';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI ?? 'mongodb://localhost:27017/crm_db'),
    MongooseModule.forFeature([
      { name: EventLog.name, schema: EventLogSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [CrmConsumer],
  providers: [KafkaProducerService],
})
export class AppModule {}

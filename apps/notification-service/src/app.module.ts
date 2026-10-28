import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { NotificationConsumer } from './notification/notification.consumer';
import { NotificationService } from './notification/notification.service';
import { KafkaProducerService } from './kafka/kafka-producer.service';
import { Notification, NotificationSchema } from './schemas/notification.schema';
import { ProcessedEvent, ProcessedEventSchema } from './schemas/processed-event.schema';

@Module({
  imports: [
    MongooseModule.forRoot(
      process.env.MONGO_URI ?? 'mongodb://localhost:27017/notification_db',
    ),
    MongooseModule.forFeature([
      { name: Notification.name, schema: NotificationSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [NotificationConsumer],
  providers: [NotificationService, KafkaProducerService],
})
export class AppModule {}

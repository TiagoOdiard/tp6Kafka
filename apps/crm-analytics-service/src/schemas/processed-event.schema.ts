import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

@Schema({ collection: 'processed_events' })
export class ProcessedEvent {
  @Prop({ type: String, required: true })
  _id: string;

  @Prop({ required: true })
  eventId: string;

  @Prop({ required: true })
  serviceName: string;

  @Prop({ required: true, default: () => new Date() })
  processedAt: Date;
}

export const ProcessedEventSchema = SchemaFactory.createForClass(ProcessedEvent);
ProcessedEventSchema.index({ processedAt: 1 }, { expireAfterSeconds: 7 * 24 * 60 * 60 });

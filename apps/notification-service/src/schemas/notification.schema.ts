import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/**
 * Colección: notifications (base notification_db)
 * Un email por activación y tipo de evento.
 */
@Schema({ collection: 'notifications' })
export class Notification {
  @Prop({ required: true })
  activationId: string;

  @Prop({ required: true })
  eventType: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  to: string;

  @Prop({ required: true })
  subject: string;

  @Prop({ required: true, enum: ['PENDING', 'SENT', 'ERROR'], default: 'PENDING' })
  status: 'PENDING' | 'SENT' | 'ERROR';

  @Prop({ required: true })
  sentAt: Date;
}

export const NotificationSchema = SchemaFactory.createForClass(Notification);
NotificationSchema.index({ activationId: 1, eventType: 1 }, { unique: true });

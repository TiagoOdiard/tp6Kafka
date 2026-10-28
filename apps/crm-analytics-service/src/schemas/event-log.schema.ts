import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/**
 * Colección: event_log (base crm_db)
 * Copia de cada evento recibido, con su topic, partición y offset.
 * Sirve para auditoría y para demostrar el replay del log de Kafka.
 */
@Schema({ collection: 'event_log' })
export class EventLog {
  @Prop({ required: true, unique: true })
  eventId: string;

  @Prop({ required: true })
  eventType: string;

  @Prop({ required: true })
  correlationId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  source: string;

  @Prop({ required: true })
  topic: string;

  @Prop({ required: true })
  partition: number;

  @Prop({ required: true })
  offset: string;

  @Prop({ required: true })
  occurredAt: Date;

  @Prop({ required: true, default: () => new Date() })
  receivedAt: Date;

  @Prop({ type: Object })
  payload: Record<string, any>;
}

export const EventLogSchema = SchemaFactory.createForClass(EventLog);

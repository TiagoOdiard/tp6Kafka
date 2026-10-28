import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/**
 * Colección: processed_events
 *
 * La usa TODO consumidor para ser idempotente (RNF-03).
 * El _id es el eventId: si se intenta insertar dos veces, MongoDB
 * devuelve error de clave duplicada (código 11000) y el evento se descarta.
 *
 * El índice TTL borra las marcas a los 7 días, igual que la retención
 * de los topics de Kafka (un evento más viejo ya no puede volver a llegar).
 */
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

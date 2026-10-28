import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/**
 * Colección: cancellations (base billing_db)
 *
 * Caso borde del PDF: ActivationFailed puede llegar ANTES de que billing
 * termine de crear la cuenta. Guardamos la intención de cancelar y, cuando
 * la cuenta se cree más tarde, se anula en el acto.
 */
@Schema({ collection: 'cancellations' })
export class Cancellation {
  @Prop({ type: String, required: true })
  _id: string; // activationId

  @Prop({ required: true })
  reason: string;

  @Prop({ required: true, default: () => new Date() })
  receivedAt: Date;
}

export const CancellationSchema = SchemaFactory.createForClass(Cancellation);

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/**
 * Colección: loyalty_accounts (base loyalty_db)
 * Se alimenta reprocesando activation.events desde el inicio del log.
 */
@Schema({ collection: 'loyalty_accounts' })
export class LoyaltyAccount {
  @Prop({ required: true, unique: true })
  customerId: string;

  @Prop({ required: true, default: 0 })
  points: number;

  @Prop({ required: true, default: 0 })
  activations: number;

  @Prop({ required: true })
  updatedAt: Date;
}

export const LoyaltyAccountSchema = SchemaFactory.createForClass(LoyaltyAccount);

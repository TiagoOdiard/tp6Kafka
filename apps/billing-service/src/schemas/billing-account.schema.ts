import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

/**
 * Colección: billing_accounts (base billing_db)
 *
 * Cuenta de facturación simulada, una por activación.
 * Estados: CREATED -> CANCELLED (compensación de la Saga).
 */
@Schema({ collection: 'billing_accounts' })
export class BillingAccount {
  @Prop({ required: true, unique: true })
  activationId: string;

  @Prop({ required: true })
  accountId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  planId: string;

  @Prop({ required: true })
  amount: number;

  @Prop({ required: true, enum: ['CREATED', 'CANCELLED'], default: 'CREATED' })
  status: 'CREATED' | 'CANCELLED';

  @Prop()
  cancelReason?: string;

  @Prop({ required: true })
  createdAt: Date;

  @Prop()
  cancelledAt?: Date;
}

export type BillingAccountDocument = HydratedDocument<BillingAccount>;
export const BillingAccountSchema = SchemaFactory.createForClass(BillingAccount);

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

/**
 * Colección: provisioning_orders (base provisioning_db)
 * Orden técnica simulada, una por activación.
 */
@Schema({ collection: 'provisioning_orders' })
export class ProvisioningOrder {
  @Prop({ required: true, unique: true })
  activationId: string;

  @Prop({ required: true })
  orderId: string;

  @Prop({ required: true })
  customerId: string;

  @Prop({ required: true })
  planId: string;

  @Prop({ required: true, default: 'COMPLETED' })
  status: string;

  @Prop({ required: true })
  createdAt: Date;
}

export const ProvisioningOrderSchema = SchemaFactory.createForClass(ProvisioningOrder);

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

/**
 * Colección: activations (base activation_db)
 *
 * Guarda el estado de la Saga, el resultado parcial de cada servicio
 * y el historial de eventos de la activación (RF-09).
 */

@Schema({ _id: false })
export class StepResult {
  @Prop({ required: true, enum: ['OK', 'FAILED'] })
  status: 'OK' | 'FAILED';

  @Prop({ required: true })
  at: Date;

  @Prop()
  reason?: string;
}

export const StepResultSchema = SchemaFactory.createForClass(StepResult);

@Schema({ _id: false })
export class HistoryEntry {
  @Prop({ required: true })
  eventId: string;

  @Prop({ required: true })
  eventType: string;

  @Prop({ required: true })
  source: string;

  @Prop({ required: true })
  at: Date;

  @Prop({ type: Object })
  payload?: Record<string, any>;
}

export const HistoryEntrySchema = SchemaFactory.createForClass(HistoryEntry);

@Schema({ _id: false })
export class ActivationSteps {
  @Prop({ type: StepResultSchema })
  billing?: StepResult;

  @Prop({ type: StepResultSchema })
  provisioning?: StepResult;
}

export const ActivationStepsSchema = SchemaFactory.createForClass(ActivationSteps);

@Schema({ collection: 'activations', versionKey: false })
export class Activation {
  @Prop({ type: String, required: true })
  _id: string;

  @Prop({ required: true, index: true })
  customerId: string;

  @Prop({ required: true })
  planId: string;

  @Prop({ required: true, default: 'none' })
  simulateFailure: 'none' | 'billing' | 'provisioning';

  @Prop({ required: true, default: 'PENDING' })
  status: 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

  @Prop({ type: ActivationStepsSchema, default: () => ({}) })
  steps: ActivationSteps;

  @Prop({ type: [HistoryEntrySchema], default: [] })
  history: HistoryEntry[];

  @Prop({ required: true })
  createdAt: Date;

  @Prop({ required: true })
  updatedAt: Date;
}

export type ActivationDocument = HydratedDocument<Activation>;

export const ActivationSchema = SchemaFactory.createForClass(Activation);

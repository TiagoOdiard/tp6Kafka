import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import nodemailer, { Transporter } from 'nodemailer';
import { EVENT_TYPES, EventEnvelope } from '@poc/contracts';
import { Notification } from '../schemas/notification.schema';
import { MAILHOG_HOST, MAILHOG_PORT } from '../kafka/kafka.config';

/**
 * Envía el email de bienvenida o de error a Mailhog (RF-07).
 *
 * Mailhog es un SMTP falso: los mails no salen a Internet, se pueden ver
 * en http://localhost:8025. Usamos el mismo patrón de idempotencia que el
 * resto: un evento repetido no manda dos emails.
 */
@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);
  private readonly transporter: Transporter;

  constructor(
    @InjectModel(Notification.name) private readonly notifications: Model<any>,
  ) {
    this.transporter = nodemailer.createTransport({
      host: MAILHOG_HOST,
      port: MAILHOG_PORT,
      secure: false,
    });
  }

  async handleActivationEvent(envelope: EventEnvelope<any>): Promise<void> {
    const isCompleted = envelope.eventType === EVENT_TYPES.ACTIVATION_COMPLETED;
    const isFailed = envelope.eventType === EVENT_TYPES.ACTIVATION_FAILED;

    if (!isCompleted && !isFailed) {
      return;
    }

    const activationId = envelope.correlationId;
    const customerId = envelope.customerId;
    const planId = envelope.payload?.planId ?? 'contratado';
    const reason = envelope.payload?.reason ?? 'Error desconocido';

    // Segunda barrera de idempotencia: una notificación por activación y tipo.
    const existing = await this.notifications.findOne({
      activationId,
      eventType: envelope.eventType,
    });
    if (existing) {
      this.logger.warn(`Notificación ya enviada para ${activationId} (${envelope.eventType})`);
      return;
    }

    const to = `${customerId.replace(/[^a-zA-Z0-9._-]/g, '')}@demo.local`;
    const subject = isCompleted
      ? `Tu plan ${planId} está activo`
      : `No pudimos activar tu plan ${planId}`;

    const html = isCompleted
      ? `<h1>¡Bienvenido/a!</h1>
         <p>Hola <b>${customerId}</b>, tu plan <b>${planId}</b> ya está activo.</p>
         <p>Gracias por confiar en nosotros.</p>`
      : `<h1>No pudimos completar la activación</h1>
         <p>Hola <b>${customerId}</b>, hubo un problema con tu plan <b>${planId}</b>.</p>
         <p>Motivo: ${reason}</p>
         <p>Si ya se había generado un cobro, fue anulado automáticamente.</p>`;

    const notification = await this.notifications.create({
      activationId,
      eventType: envelope.eventType,
      customerId,
      to,
      subject,
      status: 'PENDING',
      sentAt: new Date(),
    });

    try {
      await this.transporter.sendMail({
        from: 'no-reply@telecom.demo',
        to,
        subject,
        html,
        text: `${subject}\n\n${html.replace(/<[^>]+>/g, '')}`,
      });

      await this.notifications.updateOne({ _id: notification._id }, { $set: { status: 'SENT' } });
      this.logger.log(`Email enviado a ${to}: "${subject}" (ver http://localhost:8025)`);
    } catch (error) {
      // Si el envío falla, borramos el registro para que el reintento lo haga de nuevo.
      await this.notifications.deleteOne({ _id: notification._id });
      throw error;
    }
  }
}

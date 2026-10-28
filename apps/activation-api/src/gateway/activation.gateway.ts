import { Logger } from '@nestjs/common';
import {
  OnGatewayConnection,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { ActivationEventWsPayload } from '../kafka/kafka.config';

/**
 * Gateway WebSocket (Socket.io).
 *
 * El frontend se suscribe a `activation.event` y arma la timeline en vivo
 * (RF-08). El gateway no conoce Kafka: solo recibe lo que le pasa el servicio.
 */
@WebSocketGateway({ cors: { origin: '*' } })
export class ActivationGateway implements OnGatewayConnection {
  private readonly logger = new Logger(ActivationGateway.name);

  @WebSocketServer()
  server: Server;

  handleConnection(client: Socket): void {
    this.logger.log(`UI conectada por WebSocket: ${client.id}`);
  }

  emitActivationEvent(payload: ActivationEventWsPayload): void {
    this.server.emit('activation.event', payload);
  }
}

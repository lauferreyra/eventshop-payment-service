import { Controller } from '@nestjs/common';

import {
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';

import { randomUUID } from 'crypto';

import { PrismaService } from './prisma/prisma.service.js';
import { RabbitmqPublisherService } from './rabbitmq/rabbitmq-publisher.service.js';
import { EventEnvelope } from './events/event-envelope.js';

type InventoryReservedEvent = {
  orderId: string;
  quantity: number;
};

type InventoryReservedEnvelope =
  EventEnvelope<InventoryReservedEvent>;

@Controller()
export class AppController {
  constructor(
    private readonly rabbitmqPublisher:
      RabbitmqPublisherService,

    private readonly prisma:
      PrismaService,
  ) {}

  @EventPattern('inventory.reserved')
  async handleInventoryReserved(
    @Payload() event: InventoryReservedEnvelope,
    @Ctx() context: any,
  ) {
    const rmqContext =
      context as RmqContext;

    const channel =
      rmqContext.getChannelRef();

    const message =
      rmqContext.getMessage();

    try {
      console.log(
        '💳 Payment recibió inventory.reserved',
      );

      console.log(event);

      /*
       * 1. Registrar el evento recibido
       *    y crear el evento de salida
       *    dentro de la misma transacción.
       */
      let paymentEvent:
        | {
            eventId: string;
            eventType: string;
            version: number;
            occurredAt: string;
            data: {
              orderId: string;
              reason?: string;
            };
          }
        | undefined;

      await this.prisma.$transaction(
        async (tx) => {
          /*
           * Idempotencia:
           * intentamos registrar el eventId recibido.
           */
          try {
            await tx.processedEvent.create({
              data: {
                eventId: event.eventId,
                eventType: event.eventType,
              },
            });
          } catch (error: any) {
            if (error.code === 'P2002') {
              console.log(
                '⚠️ Evento duplicado. Ignorando:',
                event.eventId,
              );

              return;
            }

            throw error;
          }

          /*
           * Procesamiento del pago
           */
          const paymentApproved =
            event.data.quantity <= 2;

          if (paymentApproved) {
            console.log(
              '✅ Pago aprobado',
            );
          } else {
            console.log(
              '❌ Pago rechazado',
            );
          }

          /*
           * Crear evento que posteriormente
           * será publicado por el Outbox Publisher.
           */
          paymentEvent = {
            eventId: randomUUID(),

            eventType: paymentApproved
              ? 'payment.completed'
              : 'payment.failed',

            version: 1,

            occurredAt:
              new Date().toISOString(),

            data: paymentApproved
              ? {
                  orderId:
                    event.data.orderId,
                }
              : {
                  orderId:
                    event.data.orderId,

                  reason:
                    'PAYMENT_REJECTED',
                },
          };

          /*
           * Guardar evento en Outbox.
           */
          await tx.outboxEvent.create({
            data: {
              eventId:
                paymentEvent.eventId,

              eventType:
                paymentEvent.eventType,

              payload: paymentEvent,
            },
          });

          console.log(
            '📦 Evento guardado en Outbox:',
            paymentEvent.eventType,
          );
        },
      );

      /*
       * La transacción terminó correctamente.
       */
      console.log(
        '✅ Transaction COMMIT',
      );

      /*
       * ACK solamente después del COMMIT.
       */
      channel.ack(message);
    } catch (error) {
      console.error(
        '❌ Error procesando payment',
        error,
      );

      /*
       * El mensaje vuelve al flujo de
       * error/retry de RabbitMQ.
       */
      channel.nack(
        message,
        false,
        false,
      );
    }
  }
}

/*   @Get('test-transaction')
async testTransaction() {
  try {
    await this.prisma.$transaction(
      async (tx) => {
        await tx.processedEvent.create({
          data: {
            eventId: crypto.randomUUID(),
            eventType: 'transaction.test',
          },
        });

        console.log(
          '💾 Registro creado dentro de la transacción',
        );

        throw new Error(
          '💥 Error de prueba para provocar ROLLBACK',
        );
      },
    );
  } catch (error) {
    console.error(
      '❌ Transacción revertida',
      error,
    );
  }

  return {
    message: 'Prueba de transacción ejecutada',
  };
} */

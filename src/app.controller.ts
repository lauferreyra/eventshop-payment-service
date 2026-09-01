import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';

import { randomUUID } from 'crypto';

import { EventEnvelope } from './events/event-envelope.js';
import { PrismaService } from './prisma/prisma.service.js';

type InventoryReservedEvent = {
  orderId: string;
  quantity: number;
  unitPrice: number;
};

type InventoryReservedEnvelope =
  EventEnvelope<InventoryReservedEvent>;

@Controller()
export class AppController {
  constructor(

    private readonly prisma:
      PrismaService,
  ) {}

  @EventPattern('inventory.reserved')
  async handleInventoryReserved(
    @Payload()
    inventory: InventoryReservedEnvelope,

    @Ctx()
    context: any,
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

      console.log(inventory);

      const result =
        await this.prisma.$transaction(
          async (tx) => {
            /*
             * 1. Verificar idempotencia
             *
             * Si este evento ya fue procesado,
             * no volvemos a crear el Payment.
             */
            const processedEvent =
              await tx.processedEvent.findUnique({
                where: {
                  eventId:
                    inventory.eventId,
                },
              });

            if (processedEvent) {
              console.log(
                '⚠️ Evento ya procesado:',
                inventory.eventId,
              );

              return {
                alreadyProcessed: true,
              };
            }

            /*
             * 2. Calcular importe
             */
            const amount =
              inventory.data.quantity *
              inventory.data.unitPrice;

            console.log(
              '💰 Importe calculado:',
              amount,
            );

            /*
             * 3. Simular aprobación/rechazo
             *
             * Hasta 2 unidades → aprobado
             * Más de 2 → rechazado
             */
            const paymentApproved =
              inventory.data.quantity <= 2;

            const paymentStatus =
              paymentApproved
                ? 'COMPLETED'
                : 'FAILED';

            /*
             * 4. Crear Payment
             */
            const payment =
              await tx.payment.create({
                data: {
                  orderId:
                    inventory.data.orderId,

                  amount,

                  status:
                    paymentStatus,
                },
              });

            console.log(
              '💳 Payment guardado:',
              payment.id,
            );

            /*
             * 5. Determinar evento de salida
             */
            const eventType =
              paymentApproved
                ? 'payment.completed'
                : 'payment.failed';

            const eventData =
              paymentApproved
                ? {
                    orderId:
                      inventory.data.orderId,
                  }
                : {
                    orderId:
                      inventory.data.orderId,

                    reason:
                      'PAYMENT_REJECTED',
                  };

            /*
             * 6. Generar UN SOLO eventId
             *
             * Este mismo ID será utilizado:
             *
             * OutboxEvent.eventId
             * payload.eventId
             */
            const eventId =
              randomUUID();

            /*
             * 7. Crear OutboxEvent
             */
            await tx.outboxEvent.create({
              data: {
                eventId,

                eventType,

                payload: {
                  eventId,

                  eventType,

                  version: 1,

                  occurredAt:
                    new Date().toISOString(),

                  data: eventData,
                },

                status: 'PENDING',
              },
            });

            console.log(
              '📦 Evento guardado en Outbox:',
              eventType,
            );

            /*
             * 8. Registrar evento procesado
             *
             * El eventId recibido desde Inventory
             * queda registrado para evitar
             * procesamiento duplicado.
             */
            await tx.processedEvent.create({
              data: {
                eventId:
                  inventory.eventId,

                eventType:
                  inventory.eventType,
              },
            });

            console.log(
              '🆔 Evento registrado:',
              inventory.eventId,
            );

            return {
              alreadyProcessed: false,
            };
          },
        );

      /*
       * Si ya había sido procesado,
       * simplemente confirmamos el mensaje.
       */
      if (result.alreadyProcessed) {
        channel.ack(message);

        return;
      }

      /*
       * Si llegamos acá significa que:
       *
       * Payment ✅
       * Outbox ✅
       * ProcessedEvent ✅
       *
       * Todo dentro de la misma transaction.
       */
      console.log(
        '✅ Transaction COMMIT',
      );

      /*
       * El Outbox Publisher será responsable
       * de publicar payment.completed/payment.failed.
       */
      channel.ack(message);
    } catch (error) {
      console.error(
        '❌ Error procesando payment',
        error,
      );

      /*
       * Rechazamos el mensaje.
       *
       * false = no requeue
       * false = no multiple
       *
       * Nuestro sistema de retry de RabbitMQ
       * se encargará del flujo correspondiente
       * si está configurado para esta cola.
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

import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';

import { PrismaService } from '../prisma/prisma.service.js';

import { RabbitmqPublisherService } from '../rabbitmq/rabbitmq-publisher.service.js';

type OutboxEvent = {
  id: string;
  eventId: string;
  eventType: string;
  payload: unknown;
  status: 'PENDING' | 'PROCESSING' | 'PUBLISHED';
  processingAt: Date | null;
  retryCount: number;
  lastError: string | null;
  createdAt: Date;
  publishedAt: Date | null;
};

@Injectable()
export class OutboxPublisherService
  implements OnModuleInit, OnModuleDestroy
{
  private interval: NodeJS.Timeout;

  private isPublishing = false;

  private readonly BATCH_SIZE = 10;

  private readonly PROCESSING_TIMEOUT_MS =
    60 * 1000;

  private readonly POLLING_INTERVAL_MS =
    5000;

  constructor(
    private readonly prisma: PrismaService,

    private readonly rabbitmqPublisher:
      RabbitmqPublisherService,
  ) {}

  async onModuleInit() {
    console.log(
      '📦 Outbox Publisher iniciado',
    );

    await this.publishPendingEvents();

    this.interval = setInterval(
      () => {
        this.publishPendingEvents();
      },
      this.POLLING_INTERVAL_MS,
    );
  }

  private async publishPendingEvents() {
    /*
     * Evitamos que dos ejecuciones del mismo
     * proceso trabajen simultáneamente.
     */
    if (this.isPublishing) {
      return;
    }

    this.isPublishing = true;

    try {
      /*
       * 1. Recuperar eventos que quedaron
       *    PROCESSING demasiado tiempo.
       */
      await this.recoverStaleEvents();

      /*
       * 2. Reclamar eventos PENDING.
       *
       *    Acá usamos:
       *    FOR UPDATE SKIP LOCKED
       */
      const events =
        await this.claimPendingEvents();

      if (events.length === 0) {
        return;
      }

      console.log(
        `📦 Outbox: ${events.length} evento(s) reclamado(s)`,
      );

      /*
       * 3. Publicar los eventos fuera
       *    de la transacción de PostgreSQL.
       */
      for (const event of events) {
        await this.publishEvent(event);
      }
    } catch (error) {
      console.error(
        '❌ Error procesando Outbox:',
        error,
      );
    } finally {
      this.isPublishing = false;
    }
  }

  /**
   * Recupera eventos que quedaron en PROCESSING
   * porque probablemente murió un worker.
   */
  private async recoverStaleEvents() {
    const timeout = new Date(
      Date.now() -
        this.PROCESSING_TIMEOUT_MS,
    );

    const result =
      await this.prisma.outboxEvent.updateMany({
        where: {
          status: 'PROCESSING',

          processingAt: {
            lt: timeout,
          },
        },

        data: {
          status: 'PENDING',

          processingAt: null,

          lastError:
            'Processing timeout. Event returned to PENDING.',
        },
      });

    if (result.count > 0) {
      console.log(
        `♻️ Recuperados ${result.count} evento(s) abandonado(s)`,
      );
    }
  }

  /**
   * Reclama eventos pendientes.
   *
   * FOR UPDATE SKIP LOCKED permite que varios
   * workers trabajen en paralelo sin tomar
   * las mismas filas.
   */
  private async claimPendingEvents(): Promise<
    OutboxEvent[]
  > {
    return this.prisma.$transaction(
      async (tx) => {
        const events =
          await tx.$queryRaw<OutboxEvent[]>`
            SELECT
              id,
              "eventId",
              "eventType",
              payload,
              status,
              "processingAt",
              "retryCount",
              "lastError",
              "createdAt",
              "publishedAt"
            FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY "createdAt" ASC
            LIMIT ${this.BATCH_SIZE}
            FOR UPDATE SKIP LOCKED
          `;

        if (events.length === 0) {
          return [];
        }

        const ids =
          events.map(
            (event) => event.id,
          );

        await tx.outboxEvent.updateMany({
          where: {
            id: {
              in: ids,
            },
          },

          data: {
            status: 'PROCESSING',

            processingAt:
              new Date(),
          },
        });

        return events;
      },
    );
  }

  /**
   * Publica un evento y actualiza su estado.
   */
  private async publishEvent(
    event: OutboxEvent,
  ) {
    try {
      console.log(
        '📤 Outbox publicando:',
        event.eventType,
        event.eventId,
      );

      /*
       * Esperamos a que RabbitMQ confirme
       * la publicación.
       */
      await this.rabbitmqPublisher.publishRaw(
        event.eventType,
        event.payload,
      );

      /*
       * RabbitMQ confirmó.
       *
       * Ahora podemos marcar el evento
       * como PUBLISHED.
       */
      await this.prisma.outboxEvent.update({
        where: {
          id: event.id,
        },

        data: {
          status: 'PUBLISHED',

          publishedAt:
            new Date(),

          processingAt: null,

          lastError: null,
        },
      });

      console.log(
        '✅ Outbox publicó:',
        event.eventType,
        event.eventId,
      );
    } catch (error) {
      /*
       * RabbitMQ falló.
       *
       * Volvemos el evento a PENDING
       * para intentar nuevamente.
       */
      await this.prisma.outboxEvent.update({
        where: {
          id: event.id,
        },

        data: {
          status: 'PENDING',

          processingAt: null,

          retryCount: {
            increment: 1,
          },

          lastError:
            error instanceof Error
              ? error.message
              : 'Unknown error',
        },
      });

      console.error(
        '❌ Error publicando evento Outbox:',
        event.eventId,
        error,
      );
    }
  }

  async onModuleDestroy() {
    clearInterval(
      this.interval,
    );
  }
}
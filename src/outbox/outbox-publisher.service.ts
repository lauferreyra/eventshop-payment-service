import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';

import { PrismaService } from '../prisma/prisma.service.js';
import { RabbitmqPublisherService } from '../rabbitmq/rabbitmq-publisher.service.js';

@Injectable()
export class OutboxPublisherService
  implements OnModuleInit, OnModuleDestroy
{
  private interval: NodeJS.Timeout;

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
      5000,
    );
  }

  private async publishPendingEvents() {
    try {
      const events =
        await this.prisma.outboxEvent.findMany({
          where: {
            publishedAt: null,
          },

          orderBy: {
            createdAt: 'asc',
          },

          take: 10,
        });

      if (events.length === 0) {
        return;
      }

      console.log(
        `📦 Outbox: ${events.length} evento(s) pendiente(s)`,
      );

      for (const event of events) {
        try {
          await this.rabbitmqPublisher.publishRaw(
            event.eventType,
            event.payload,
          );

          await this.prisma.outboxEvent.update({
            where: {
              id: event.id,
            },

            data: {
              publishedAt: new Date(),
            },
          });

          console.log(
            '📤 Outbox publicó:',
            event.eventType,
            event.eventId,
          );
        } catch (error) {
          console.error(
            '❌ Error publicando evento Outbox:',
            event.eventId,
            error,
          );
        }
      }
    } catch (error) {
      console.error(
        '❌ Error leyendo Outbox:',
        error,
      );
    }
  }

  async onModuleDestroy() {
    clearInterval(this.interval);
  }
}
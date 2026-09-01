import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';

import * as amqp from 'amqplib';

import {
  ConfirmChannel,
  ChannelModel,
} from 'amqplib';

@Injectable()
export class RabbitmqPublisherService
  implements OnModuleInit, OnModuleDestroy
{
  private connection: ChannelModel;

  private channel: ConfirmChannel;

  private readonly exchange =
    'eventshop.events';

  async onModuleInit() {
    this.connection =
      await amqp.connect(
        'amqp://admin:admin@localhost:5672',
      );

    /*
     * Creamos un ConfirmChannel
     * en lugar de un Channel normal.
     */
    this.channel =
      await this.connection.createConfirmChannel();

    await this.channel.assertExchange(
      this.exchange,
      'topic',
      {
        durable: true,
      },
    );

    console.log(
      '✅ Payment Publisher conectado a RabbitMQ',
    );
  }

  publish<T>(
    eventType: string,
    data: T,
  ) {
    console.log(
      '📤 Payment publicando:',
      eventType,
      data,
    );

    const event = {
      eventId:
        crypto.randomUUID(),

      eventType,

      version: 1,

      occurredAt:
        new Date().toISOString(),

      data,
    };

    const message =
      Buffer.from(
        JSON.stringify(event),
      );

    this.channel.publish(
      this.exchange,
      eventType,
      message,
      {
        persistent: true,

        contentType:
          'application/json',
      },
    );
  }

  publishRaw(
    eventType: string,
    payload: unknown,
  ): Promise<void> {
    console.log(
      '📤 Outbox publicando:',
      eventType,
    );

    const message =
      Buffer.from(
        JSON.stringify(payload),
      );

    return new Promise(
      (resolve, reject) => {
        this.channel.publish(
          this.exchange,
          eventType,
          message,
          {
            persistent: true,

            contentType:
              'application/json',
          },

          (error) => {
            if (error) {
              console.error(
                '❌ RabbitMQ rechazó el mensaje',
                error,
              );

              reject(error);

              return;
            }

            console.log(
              '✅ RabbitMQ confirmó:',
              eventType,
            );

            resolve();
          },
        );
      },
    );
  }

  async onModuleDestroy() {
    await this.channel?.close();

    await this.connection?.close();
  }
}
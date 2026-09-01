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
     * Usamos ConfirmChannel para poder
     * esperar la confirmación de RabbitMQ.
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

  /*
   * Publisher utilizado por el Outbox.
   *
   * Esperamos confirmación de RabbitMQ.
   */
  publishRaw(
    eventType: string,
    payload: unknown,
  ): Promise<void> {
    console.log(
      '📤 Outbox publicando:',
      eventType,
    );

    /*
     * Nest necesita recibir:
     *
     * {
     *   pattern: 'payment.completed',
     *   data: ...
     * }
     */
    const message = {
      pattern: eventType,

      data: payload,
    };

    const buffer =
      Buffer.from(
        JSON.stringify(message),
      );

    return new Promise(
      (resolve, reject) => {
        this.channel.publish(
          this.exchange,
          eventType,
          buffer,
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
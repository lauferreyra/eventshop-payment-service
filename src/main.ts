import { NestFactory } from '@nestjs/core';
import {
  MicroserviceOptions,
  Transport,
} from '@nestjs/microservices';

import { AppModule } from './app.module.js';

async function bootstrap() {
  const app =
    await NestFactory.createMicroservice<MicroserviceOptions>(
      AppModule,
      {
        transport: Transport.RMQ,
        options: {
           urls: [
            process.env.RABBITMQ_URL ??
              'amqp://admin:admin@localhost:5672',
          ],

          queue: 'payment_queue',

          queueOptions: {
            durable: true,
          },

          noAck: false,
        },
      },
    );

  await app.listen();
}

bootstrap();
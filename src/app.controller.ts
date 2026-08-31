import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';

import { RabbitmqPublisherService } from './rabbitmq/rabbitmq-publisher.service.js';

type InventoryReservedEvent = {
  orderId: string;
  quantity: number;
};

@Controller()
export class AppController {
  constructor(
    private readonly rabbitmqPublisher:
      RabbitmqPublisherService,
  ) {}

  @EventPattern('inventory.reserved')
  handleInventoryReserved(
    @Payload() inventory: InventoryReservedEvent,
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

      console.log(inventory);

      const paymentApproved =
        inventory.quantity <= 2;

      if (paymentApproved) {
        console.log(
          '✅ Pago aprobado',
        );

        this.rabbitmqPublisher.publish(
          'payment.completed',
          {
            orderId: inventory.orderId,
          },
        );
      } else {
        console.log(
          '❌ Pago rechazado',
        );

        this.rabbitmqPublisher.publish(
          'payment.failed',
          {
            orderId: inventory.orderId,
            reason: 'PAYMENT_REJECTED',
          },
        );
      }

      channel.ack(message);
    } catch (error) {
      console.error(
        '❌ Error procesando payment',
        error,
      );

      channel.nack(
        message,
        false,
        false,
      );
    }
  }
}
import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  Payload,
  RmqContext,
} from '@nestjs/microservices';

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
  ) {}

  @EventPattern('inventory.reserved')
  handleInventoryReserved(
    @Payload() inventory: InventoryReservedEnvelope,
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
        inventory.data.quantity <= 2;

      if (paymentApproved) {
        console.log(
          '✅ Pago aprobado',
        );

        this.rabbitmqPublisher.publish(
          'payment.completed',
          {
            orderId: inventory.data.orderId,
          },
        );
      } else {
        console.log(
          '❌ Pago rechazado',
        );

        this.rabbitmqPublisher.publish(
          'payment.failed',
          {
            orderId: inventory.data.orderId,
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
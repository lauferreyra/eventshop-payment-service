import { Module } from '@nestjs/common';

import { OutboxPublisherService } from './outbox-publisher.service.js';
import { RabbitmqModule } from '../rabbitmq/rabbitmq.module.js';

@Module({
  imports: [
    RabbitmqModule,
  ],

  providers: [
    OutboxPublisherService,
  ],
})
export class OutboxModule {}
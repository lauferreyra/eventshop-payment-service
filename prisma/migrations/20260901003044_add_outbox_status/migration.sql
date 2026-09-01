-- CreateEnum
CREATE TYPE "OutboxStatus" AS ENUM ('PENDING', 'PROCESSING', 'PUBLISHED');

-- AlterTable
ALTER TABLE "outbox_events" ADD COLUMN     "status" "OutboxStatus" NOT NULL DEFAULT 'PENDING';

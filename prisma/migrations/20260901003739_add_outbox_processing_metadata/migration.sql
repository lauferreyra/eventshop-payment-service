-- AlterTable
ALTER TABLE "outbox_events" ADD COLUMN     "lastError" TEXT,
ADD COLUMN     "processingAt" TIMESTAMP(3),
ADD COLUMN     "retryCount" INTEGER NOT NULL DEFAULT 0;

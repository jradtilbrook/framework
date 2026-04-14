<?php

namespace Illuminate\Queue;

use Illuminate\Foundation\LaravelCloudSocket;

class CloudQueueEventEmitter
{
    /**
     * The processing start time.
     *
     * @var float|null
     */
    protected static $processingStartedAt;

    /**
     * Emit a queued job event.
     */
    public static function queued(string $queue, float $delay): void
    {
        static::emit([
            'type' => 'job.queued',
            'timestamp' => static::timestamp(),
            'queue' => $queue,
            'delay' => $delay,
        ]);
    }

    /**
     * Emit a job processing event and begin duration timing.
     */
    public static function processing(string $queue, ?float $wait): void
    {
        static::$processingStartedAt = microtime(true);

        static::emit([
            'type' => 'job.processing',
            'timestamp' => static::timestamp(),
            'queue' => $queue,
            'wait' => $wait,
        ]);
    }

    /**
     * Emit a job processed event and stop duration timing.
     */
    public static function processed(string $queue): void
    {
        $duration = static::durationSeconds();

        static::emit([
            'type' => 'job.processed',
            'timestamp' => static::timestamp(),
            'queue' => $queue,
            'duration' => $duration,
        ]);

        static::forgetDuration();
    }

    /**
     * Emit a job released event and stop duration timing.
     */
    public static function released(string $queue, int $backoff): void
    {
        $duration = static::durationSeconds();

        static::emit([
            'type' => 'job.released',
            'timestamp' => static::timestamp(),
            'queue' => $queue,
            'backoff' => $backoff,
            'duration' => $duration,
        ]);

        static::forgetDuration();
    }

    /**
     * Emit a job failed event and stop duration timing.
     */
    public static function failed(string $queue): void
    {
        $duration = static::durationSeconds();

        static::emit([
            'type' => 'job.failed',
            'timestamp' => static::timestamp(),
            'queue' => $queue,
            'duration' => $duration,
        ]);

        static::forgetDuration();
    }

    /**
     * Get the current timestamp in seconds.
     */
    protected static function timestamp(): float
    {
        return microtime(true);
    }

    /**
     * Get the queue duration in seconds.
     */
    protected static function durationSeconds(): ?float
    {
        if (static::$processingStartedAt === null) {
            return null;
        }

        return max(microtime(true) - static::$processingStartedAt, 0.0);
    }

    /**
     * Forget a queue duration start time.
     */
    protected static function forgetDuration(): void
    {
        static::$processingStartedAt = null;
    }

    /**
     * Emit a queue event.
     */
    protected static function emit(array $record): void
    {
        LaravelCloudSocket::writeJson($record);
    }
}

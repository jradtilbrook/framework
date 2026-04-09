<?php

namespace Illuminate\Queue;

class CloudQueueEventEmitter
{
    /**
     * The processing start times keyed by job identifier.
     *
     * @var array<string, float>
     */
    protected static $processingStartedAt = [];

    /**
     * The socket stream resource.
     *
     * @var resource|null
     */
    protected static $socket;

    /**
     * The configured socket connection string.
     *
     * @var string|null
     */
    protected static $socketConnection;

    /**
     * The callback used to write the event payload.
     *
     * @var callable|null
     */
    protected static $writeUsing;

    /**
     * Whether the emitter is enabled.
     *
     * @var bool
     */
    protected static $enabled = false;

    /**
     * Configure the cloud queue event emitter.
     */
    public static function configure(?string $socketConnection = null): void
    {
        static::$enabled = true;
        static::$socketConnection = $socketConnection;
    }

    /**
     * Disable the cloud queue event emitter.
     */
    public static function disable(): void
    {
        static::$enabled = false;
        static::$socketConnection = null;
        static::$socket = null;
    }

    /**
     * Set the callback used to write payloads.
     */
    public static function writeUsing(?callable $callback): void
    {
        static::$writeUsing = $callback;
    }

    /**
     * Emit a queued job event.
     */
    public static function queued(float $delay): void
    {
        if (! static::$enabled) {
            return;
        }

        static::emit([
            'type' => 'job.queued',
            'timestamp' => static::timestamp(),
            'delay' => $delay,
        ]);
    }

    /**
     * Emit a job processing event and begin duration timing.
     */
    public static function processing(float $wait): void
    {
        if (! static::$enabled) {
            return;
        }

        static::$processingStartedAt['sqs'] = microtime(true);

        static::emit([
            'type' => 'job.processing',
            'timestamp' => static::timestamp(),
            'wait' => $wait,
        ]);
    }

    /**
     * Emit a job processed event and stop duration timing.
     */
    public static function processed(): void
    {
        if (! static::$enabled) {
            return;
        }

        $duration = static::durationSeconds();

        static::emit([
            'type' => 'job.processed',
            'timestamp' => static::timestamp(),
            'duration' => $duration,
        ]);

        static::forgetDuration();
    }

    /**
     * Emit a job released event and stop duration timing.
     */
    public static function released(int $backoff): void
    {
        if (! static::$enabled) {
            return;
        }

        $duration = static::durationSeconds();

        static::emit([
            'type' => 'job.released',
            'timestamp' => static::timestamp(),
            'backoff' => $backoff,
            'duration' => $duration,
        ]);

        static::forgetDuration();
    }

    /**
     * Emit a job failed event and stop duration timing.
     */
    public static function failed(): void
    {
        if (! static::$enabled) {
            return;
        }

        $duration = static::durationSeconds();

        static::emit([
            'type' => 'job.failed',
            'timestamp' => static::timestamp(),
            'duration' => $duration,
        ]);

        static::forgetDuration();
    }

    /**
     * Emit a job timed out event and stop duration timing.
     */
    public static function timedOut(): void
    {
        if (! static::$enabled) {
            return;
        }

        $duration = static::durationSeconds();

        static::emit([
            'type' => 'job.timed_out',
            'timestamp' => static::timestamp(),
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
        if (! isset(static::$processingStartedAt['sqs'])) {
            return null;
        }

        return max(microtime(true) - static::$processingStartedAt['sqs'], 0.0);
    }

    /**
     * Forget a queue duration start time.
     */
    protected static function forgetDuration(): void
    {
        unset(static::$processingStartedAt['sqs']);
    }

    /**
     * Emit a queue event.
     */
    protected static function emit(array $record): void
    {
        $payload = \json_encode($record, \JSON_UNESCAPED_SLASHES | \JSON_UNESCAPED_UNICODE);

        if ($payload === false) {
            return;
        }

        if (\is_callable(static::$writeUsing)) {
            (static::$writeUsing)($payload);

            return;
        }

        static::writeToSocket($payload.\PHP_EOL);
    }

    /**
     * Write a payload to the configured socket.
     */
    protected static function writeToSocket(string $payload): void
    {
        $socket = static::socket();

        if (! is_resource($socket)) {
            return;
        }

        if (@\fwrite($socket, $payload) === false) {
            @\fclose($socket);

            static::$socket = null;
        }
    }

    /**
     * Get the socket resource.
     *
     * @return resource|null
     */
    protected static function socket()
    {
        if (\is_resource(static::$socket)) {
            return static::$socket;
        }

        if (! \is_string(static::$socketConnection) || static::$socketConnection === '') {
            return null;
        }

        $socket = @\stream_socket_client(
            static::$socketConnection,
            $errorCode,
            $errorMessage,
            0.2,
            \STREAM_CLIENT_CONNECT | \STREAM_CLIENT_PERSISTENT,
        );

        if (! \is_resource($socket)) {
            return null;
        }

        @\stream_set_blocking($socket, false);

        return static::$socket = $socket;
    }
}

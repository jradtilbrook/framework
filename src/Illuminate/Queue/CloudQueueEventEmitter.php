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
    public static function queued(?string $connectionName, float $delay): void
    {
        if (! static::shouldEmit($connectionName)) {
            return;
        }

        static::emit([
            'event' => 'queued',
            'ts' => static::timestamp(),
            'delay' => $delay,
        ]);
    }

    /**
     * Emit a job processing event and begin duration timing.
     */
    public static function processing(?string $connectionName, float $wait): void
    {
        if (! static::shouldEmit($connectionName)) {
            return;
        }

        $key = $connectionName;

        static::$processingStartedAt[$key] = microtime(true);

        static::emit([
            'event' => 'processing',
            'ts' => static::timestamp(),
            'wait' => $wait,
        ]);
    }

    /**
     * Emit a job processed event and stop duration timing.
     */
    public static function processed(?string $connectionName): void
    {
        if (! static::shouldEmit($connectionName)) {
            return;
        }

        $key = $connectionName;
        $duration = static::durationSeconds($key);

        static::emit([
            'event' => 'processed',
            'ts' => static::timestamp(),
            'duration' => $duration,
        ]);

        static::forgetDuration($key);
    }

    /**
     * Emit a job released event and stop duration timing.
     */
    public static function released(?string $connectionName, int $backoff): void
    {
        if (! static::shouldEmit($connectionName)) {
            return;
        }

        $key = $connectionName;
        $duration = static::durationSeconds($key);

        static::emit([
            'event' => 'released',
            'ts' => static::timestamp(),
            'backoff' => $backoff,
            'duration' => $duration,
        ]);

        static::forgetDuration($key);
    }

    /**
     * Emit a job failed event and stop duration timing.
     */
    public static function failed(?string $connectionName): void
    {
        if (! static::shouldEmit($connectionName)) {
            return;
        }

        $key = $connectionName;
        $duration = static::durationSeconds($key);

        static::emit([
            'event' => 'failed',
            'ts' => static::timestamp(),
            'duration' => $duration,
        ]);

        static::forgetDuration($key);
    }

    /**
     * Emit a job timed out event and stop duration timing.
     */
    public static function timedOut(?string $connectionName): void
    {
        if (! static::shouldEmit($connectionName)) {
            return;
        }

        $key = $connectionName;
        $duration = static::durationSeconds($key);

        static::emit([
            'event' => 'timed_out',
            'ts' => static::timestamp(),
            'duration' => $duration,
        ]);

        static::forgetDuration($key);
    }

    /**
     * Determine if events should be emitted for the connection.
     */
    protected static function shouldEmit(?string $connectionName): bool
    {
        if (! static::$enabled) {
            return false;
        }

        if ($connectionName === null) {
            return false;
        }

        // Only emit for SQS driver
        return $connectionName === 'sqs' || str_starts_with($connectionName, 'sqs');
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
    protected static function durationSeconds(string $key): ?float
    {
        if (! isset(static::$processingStartedAt[$key])) {
            return null;
        }

        return max(microtime(true) - static::$processingStartedAt[$key], 0.0);
    }

    /**
     * Forget a queue duration start time.
     */
    protected static function forgetDuration(string $key): void
    {
        unset(static::$processingStartedAt[$key]);
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

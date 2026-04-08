<?php

namespace Illuminate\Queue;

use Illuminate\Contracts\Container\Container;

class CloudQueueEventEmitter
{
    /**
     * The queue event schema version.
     *
     * @var string
     */
    protected const SCHEMA_VERSION = '1.0';

    /**
     * The queue event marker.
     *
     * @var string
     */
    protected const KIND = 'laravel.queue.event';

    /**
     * The maximum error message length.
     *
     * @var int
     */
    protected const ERROR_MESSAGE_LIMIT = 500;

    /**
     * The callback used to write the event payload.
     *
     * @var callable|null
     */
    protected static $writeUsing;

    /**
     * The configured emitter instance.
     *
     * @var static|null
     */
    protected static $instance;

    /**
     * The processing start times keyed by job identifier.
     *
     * @var array<string, float>
     */
    protected $processingStartedAt = [];

    /**
     * The socket stream resource.
     *
     * @var resource|null
     */
    protected $socket;

    /**
     * Create a new queue event emitter instance.
     */
    public function __construct(
        protected Container $app,
        protected ?string $socketConnection = null,
    ) {
    }

    /**
     * Configure the cloud queue event emitter.
     */
    public static function configure(Container $app, ?string $socketConnection = null): void
    {
        static::$instance = new static($app, $socketConnection);
    }

    /**
     * Disable the cloud queue event emitter.
     */
    public static function disable(): void
    {
        static::$instance = null;
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
     *
     * @param  string|int|null  $jobId
     * @param  \Closure|string|object  $job
     * @param  int|null  $delay
     */
    public static function queued(?string $connectionName, ?string $queue, $jobId, $job, string $payload, $delay): void
    {
        static::forConnection($connectionName)?->safely(function ($emitter) use ($connectionName, $queue, $jobId, $job, $payload, $delay) {
            $decodedPayload = $emitter->decodePayload($payload);

            $emitter->emit([
                ...$emitter->baseRecord('job.queued', $connectionName, $queue),
                'messaging.operation.name' => 'send',
                'job.uuid' => $emitter->toString($decodedPayload['uuid'] ?? null),
                'job.id' => $emitter->toIdentifier($jobId),
                'job.name' => $emitter->jobName($decodedPayload['displayName'] ?? $job),
                'laravel.queue.attempt' => $emitter->toInteger($decodedPayload['attempts'] ?? null),
                'laravel.queue.delay_s' => $emitter->toInteger($delay),
                'laravel.queue.created_at_unix' => $emitter->toInteger($decodedPayload['createdAt'] ?? null),
                ...$emitter->traceContextFromPayload($decodedPayload),
            ]);
        });
    }

    /**
     * Emit a job processing event and begin duration timing.
     */
    public static function processing(?string $connectionName, $job): void
    {
        static::forConnection($connectionName)?->safely(function ($emitter) use ($connectionName, $job) {
            $jobContext = $emitter->jobContext($job);
            $key = $emitter->processingKey($jobContext['job.uuid'], $jobContext['job.id']);

            if ($key !== null) {
                $emitter->processingStartedAt[$key] = \microtime(true);
            }

            $payload = $emitter->call($job, 'payload');

            $emitter->emit([
                ...$emitter->baseRecord('job.processing', $connectionName, $jobContext['queue']),
                'messaging.operation.name' => 'process',
                ...$emitter->jobRecord($jobContext),
                'laravel.queue.wait_ms' => $emitter->waitMilliseconds($job),
                ...$emitter->traceContextFromPayload(\is_array($payload) ? $payload : []),
            ]);
        });
    }

    /**
     * Emit a job processed event and stop duration timing.
     */
    public static function processed(?string $connectionName, $job): void
    {
        static::forConnection($connectionName)?->safely(function ($emitter) use ($connectionName, $job) {
            $jobContext = $emitter->jobContext($job);
            $key = $emitter->processingKey($jobContext['job.uuid'], $jobContext['job.id']);
            $payload = $emitter->call($job, 'payload');

            $emitter->emit([
                ...$emitter->baseRecord('job.processed', $connectionName, $jobContext['queue']),
                'messaging.operation.name' => 'process',
                ...$emitter->jobRecord($jobContext),
                'laravel.queue.wait_ms' => $emitter->waitMilliseconds($job),
                'laravel.queue.duration_ms' => $emitter->durationMilliseconds($key),
                'laravel.queue.result' => $emitter->call($job, 'isDeleted') ? 'deleted' : 'processed',
                ...$emitter->traceContextFromPayload(\is_array($payload) ? $payload : []),
            ]);

            $emitter->forgetDuration($key);
        });
    }

    /**
     * Emit a job released event and stop duration timing.
     *
     * @param  int|null  $backoff
     */
    public static function released(?string $connectionName, $job, $backoff): void
    {
        static::forConnection($connectionName)?->safely(function ($emitter) use ($connectionName, $job, $backoff) {
            $jobContext = $emitter->jobContext($job);
            $key = $emitter->processingKey($jobContext['job.uuid'], $jobContext['job.id']);
            $payload = $emitter->call($job, 'payload');

            $emitter->emit([
                ...$emitter->baseRecord('job.released', $connectionName, $jobContext['queue']),
                'messaging.operation.name' => 'settle',
                ...$emitter->jobRecord($jobContext),
                'laravel.queue.backoff_s' => $emitter->toInteger($backoff),
                'laravel.queue.duration_ms' => $emitter->durationMilliseconds($key),
                'laravel.queue.result' => 'released',
                ...$emitter->traceContextFromPayload(\is_array($payload) ? $payload : []),
            ]);

            $emitter->forgetDuration($key);
        });
    }

    /**
     * Emit a job failed event and stop duration timing.
     */
    public static function failed(?string $connectionName, $job, ?\Throwable $exception): void
    {
        static::forConnection($connectionName)?->safely(function ($emitter) use ($connectionName, $job, $exception) {
            $jobContext = $emitter->jobContext($job);
            $key = $emitter->processingKey($jobContext['job.uuid'], $jobContext['job.id']);
            $payload = $emitter->call($job, 'payload');

            $emitter->emit([
                ...$emitter->baseRecord('job.failed', $connectionName, $jobContext['queue']),
                'messaging.operation.name' => 'settle',
                ...$emitter->jobRecord($jobContext),
                'laravel.queue.duration_ms' => $emitter->durationMilliseconds($key),
                'laravel.queue.result' => 'failed',
                'error.type' => $exception ? $exception::class : null,
                'error.message' => $emitter->truncateErrorMessage($exception?->getMessage()),
                ...$emitter->traceContextFromPayload(\is_array($payload) ? $payload : []),
            ]);

            $emitter->forgetDuration($key);
        });
    }

    /**
     * Emit a job timed out event and stop duration timing.
     */
    public static function timedOut(?string $connectionName, $job): void
    {
        static::forConnection($connectionName)?->safely(function ($emitter) use ($connectionName, $job) {
            $jobContext = $emitter->jobContext($job);
            $key = $emitter->processingKey($jobContext['job.uuid'], $jobContext['job.id']);
            $payload = $emitter->call($job, 'payload');

            $emitter->emit([
                ...$emitter->baseRecord('job.timed_out', $connectionName, $jobContext['queue']),
                'messaging.operation.name' => 'process',
                ...$emitter->jobRecord($jobContext),
                'laravel.queue.duration_ms' => $emitter->durationMilliseconds($key),
                'laravel.queue.result' => 'timed_out',
                ...$emitter->traceContextFromPayload(\is_array($payload) ? $payload : []),
            ]);

            $emitter->forgetDuration($key);
        });
    }

    /**
     * Get the configured emitter for the given connection.
     */
    protected static function forConnection(?string $connectionName): ?self
    {
        if (! \laravel_cloud()) {
            return null;
        }

        if (! static::$instance instanceof self) {
            return null;
        }

        return static::$instance->shouldEmitForConnection($connectionName)
            ? static::$instance
            : null;
    }

    /**
     * Determine if queue events should be emitted for the given connection.
     */
    protected function shouldEmitForConnection(?string $connectionName): bool
    {
        $connectionName = $this->toString($connectionName) ?? 'unknown';

        if ($connectionName === 'sync') {
            return false;
        }

        return $this->messagingSystem($connectionName) !== 'sync';
    }

    /**
     * Get the base queue event record.
     *
     * @param  string|null  $queue
     * @return array<string, mixed>
     */
    protected function baseRecord(string $eventName, ?string $connectionName, ?string $queue): array
    {
        $connectionName = $this->toString($connectionName) ?? 'unknown';
        $queue = $this->toString($queue) ?? 'default';

        return [
            'schema_version' => static::SCHEMA_VERSION,
            'kind' => static::KIND,
            'event_name' => $eventName,
            'timestamp' => $this->timestamp(),
            'service.name' => (string) $this->config('app.name', 'laravel'),
            'deployment.environment.name' => (string) $this->config('app.env', 'production'),
            'laravel.queue.connection' => $connectionName,
            'messaging.system' => $this->messagingSystem($connectionName),
            'messaging.destination.name' => $queue,
        ];
    }

    /**
     * Get the queue event job record.
     *
     * @param  array<string, mixed>  $job
     * @return array<string, mixed>
     */
    protected function jobRecord(array $job): array
    {
        return [
            'job.uuid' => $job['job.uuid'],
            'job.id' => $job['job.id'],
            'job.name' => $job['job.name'],
            'laravel.queue.attempt' => $job['laravel.queue.attempt'],
        ];
    }

    /**
     * Get the queue event job context.
     *
     * @return array<string, mixed>
     */
    protected function jobContext($job): array
    {
        return [
            'job.uuid' => $this->toString($this->call($job, 'uuid')),
            'job.id' => $this->toIdentifier($this->call($job, 'getJobId')),
            'job.name' => $this->jobName($this->call($job, 'resolveName') ?? $job),
            'queue' => $this->toString($this->call($job, 'getQueue')),
            'laravel.queue.attempt' => $this->toInteger($this->call($job, 'attempts')),
        ];
    }

    /**
     * Get trace context fields from a queue payload.
     *
     * @param  array<string, mixed>  $payload
     * @return array<string, string>
     */
    protected function traceContextFromPayload(array $payload): array
    {
        $traceparent = $this->toString($payload['traceparent'] ?? null)
            ?? $this->toString($payload['data']['traceparent'] ?? null);

        if ($traceparent === null) {
            return [];
        }

        $matches = [];

        if (! \preg_match('/^[0-9a-f]{2}-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/i', $traceparent, $matches)) {
            return [];
        }

        $traceContext = [
            'trace_id' => \strtolower($matches[1]),
            'span_id' => \strtolower($matches[2]),
            'trace_flags' => \strtolower($matches[3]),
        ];

        $tracestate = $this->toString($payload['tracestate'] ?? null)
            ?? $this->toString($payload['data']['tracestate'] ?? null);

        if ($tracestate !== null) {
            $traceContext['tracestate'] = $tracestate;
        }

        return $traceContext;
    }

    /**
     * Get the queue event messaging system.
     */
    protected function messagingSystem(string $connectionName): string
    {
        return $this->toString(
            $this->config("queue.connections.{$connectionName}.driver")
        ) ?? $connectionName;
    }

    /**
     * Get the current timestamp.
     */
    protected function timestamp(): string
    {
        return (new \DateTimeImmutable('now', new \DateTimeZone('UTC')))
            ->format('Y-m-d\TH:i:s.u\Z');
    }

    /**
     * Emit a queue event.
     *
     * @param  array<string, mixed>  $record
     */
    protected function emit(array $record): void
    {
        $payload = \json_encode($record, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);

        if ($payload === false) {
            return;
        }

        if (\is_callable(static::$writeUsing)) {
            (static::$writeUsing)($payload);

            return;
        }

        $this->writeToSocket($payload.PHP_EOL);
    }

    /**
     * Execute a callback safely.
     */
    protected function safely(callable $callback): void
    {
        try {
            $callback($this);
        } catch (\Throwable) {
            // Ignore queue emission errors so queue processing can continue.
        }
    }

    /**
     * Write a payload to the configured socket.
     */
    protected function writeToSocket(string $payload): void
    {
        $socket = $this->socket();

        if (! \is_resource($socket)) {
            return;
        }

        if (@\fwrite($socket, $payload) === false) {
            @\fclose($socket);

            $this->socket = null;
        }
    }

    /**
     * Get the socket resource.
     *
     * @return resource|null
     */
    protected function socket()
    {
        if (\is_resource($this->socket)) {
            return $this->socket;
        }

        if (! \is_string($this->socketConnection) || $this->socketConnection === '') {
            return null;
        }

        $socket = @\stream_socket_client(
            $this->socketConnection,
            $errorCode,
            $errorMessage,
            0.2,
            STREAM_CLIENT_CONNECT | STREAM_CLIENT_PERSISTENT,
        );

        if (! \is_resource($socket)) {
            return null;
        }

        @\stream_set_blocking($socket, false);

        return $this->socket = $socket;
    }

    /**
     * Decode a queue payload.
     *
     * @return array<string, mixed>
     */
    protected function decodePayload($payload): array
    {
        if (! \is_string($payload) || $payload === '') {
            return [];
        }

        $decoded = \json_decode($payload, true);

        return \is_array($decoded) ? $decoded : [];
    }

    /**
     * Determine the queue event processing key.
     *
     * @param  string|int|null  $jobId
     */
    protected function processingKey(?string $uuid, $jobId): ?string
    {
        if ($uuid !== null && $uuid !== '') {
            return 'uuid:'.$uuid;
        }

        if (! \is_null($jobId) && $jobId !== '') {
            return 'id:'.(string) $jobId;
        }

        return null;
    }

    /**
     * Get the queue wait time in milliseconds.
     */
    protected function waitMilliseconds($job): ?int
    {
        $payload = $this->call($job, 'payload');

        if (! \is_array($payload)) {
            return null;
        }

        $createdAt = $this->toInteger($payload['createdAt'] ?? null);

        if ($createdAt === null) {
            return null;
        }

        return \max((int) \round((\microtime(true) - $createdAt) * 1000), 0);
    }

    /**
     * Get the queue duration in milliseconds.
     */
    protected function durationMilliseconds(?string $key): ?int
    {
        if ($key === null || ! isset($this->processingStartedAt[$key])) {
            return null;
        }

        return \max((int) \round((\microtime(true) - $this->processingStartedAt[$key]) * 1000), 0);
    }

    /**
     * Forget a queue duration start time.
     */
    protected function forgetDuration(?string $key): void
    {
        if ($key === null) {
            return;
        }

        unset($this->processingStartedAt[$key]);
    }

    /**
     * Resolve the queue event job name.
     */
    protected function jobName($job): string
    {
        if (\is_string($job) && $job !== '') {
            return $job;
        }

        if ($job instanceof \Closure) {
            return 'Closure';
        }

        if (\is_object($job)) {
            return $job::class;
        }

        return 'Unknown';
    }

    /**
     * Truncate a queue error message.
     */
    protected function truncateErrorMessage(?string $message): ?string
    {
        if ($message === null || $message === '') {
            return null;
        }

        if (\mb_strlen($message) <= static::ERROR_MESSAGE_LIMIT) {
            return $message;
        }

        return \mb_substr($message, 0, static::ERROR_MESSAGE_LIMIT);
    }

    /**
     * Convert a value to a string.
     */
    protected function toString($value): ?string
    {
        if (! \is_string($value) || $value === '') {
            return null;
        }

        return $value;
    }

    /**
     * Convert a value to an integer.
     */
    protected function toInteger($value): ?int
    {
        if (! \is_numeric($value)) {
            return null;
        }

        return (int) $value;
    }

    /**
     * Convert a value to a queue identifier.
     *
     * @return string|int|null
     */
    protected function toIdentifier($value)
    {
        return \is_string($value) || \is_int($value)
            ? $value
            : null;
    }

    /**
     * Call the given method on the target if possible.
     */
    protected function call($target, string $method)
    {
        if (! \is_object($target) || ! \method_exists($target, $method)) {
            return null;
        }

        try {
            return $target->{$method}();
        } catch (\Throwable) {
            return null;
        }
    }

    /**
     * Get a configuration value.
     *
     * @param  mixed  $default
     * @return mixed
     */
    protected function config(string $key, $default = null)
    {
        try {
            return $this->app->make('config')->get($key, $default);
        } catch (\Throwable) {
            return $default;
        }
    }
}

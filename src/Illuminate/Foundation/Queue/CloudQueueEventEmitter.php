<?php

namespace Illuminate\Foundation\Queue;

use DateTimeImmutable;
use DateTimeZone;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Foundation\Application;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Queue\Events\JobReleasedAfterException;
use Illuminate\Queue\Events\JobTimedOut;
use Throwable;

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
     * The processing start times keyed by job identifier.
     *
     * @var array<string, float>
     */
    protected $processingStartedAt = [];

    /**
     * Create a new queue event emitter instance.
     */
    public function __construct(protected Application $app)
    {
    }

    /**
     * Register the queue event listeners.
     */
    public function register(Dispatcher $events): void
    {
        $events->listen(JobQueued::class, fn (JobQueued $event) => $this->safely(fn () => $this->handleQueued($event)));
        $events->listen(JobProcessing::class, fn (JobProcessing $event) => $this->safely(fn () => $this->handleProcessing($event)));
        $events->listen(JobProcessed::class, fn (JobProcessed $event) => $this->safely(fn () => $this->handleProcessed($event)));
        $events->listen(JobReleasedAfterException::class, fn (JobReleasedAfterException $event) => $this->safely(fn () => $this->handleReleased($event)));
        $events->listen(JobFailed::class, fn (JobFailed $event) => $this->safely(fn () => $this->handleFailed($event)));
        $events->listen(JobTimedOut::class, fn (JobTimedOut $event) => $this->safely(fn () => $this->handleTimedOut($event)));
    }

    /**
     * Set the callback used to write payloads.
     */
    public static function writeUsing(?callable $callback): void
    {
        static::$writeUsing = $callback;
    }

    /**
     * Handle a queued event.
     */
    protected function handleQueued(JobQueued $event): void
    {
        $payload = $this->decodePayload($event->payload);

        $this->emit([
            ...$this->baseRecord('job.queued', $event->connectionName, $event->queue),
            'messaging.operation.name' => 'send',
            'job.uuid' => $this->toString($payload['uuid'] ?? null),
            'job.id' => $this->toIdentifier($event->id),
            'job.name' => $this->jobName($payload['displayName'] ?? $event->job),
            'laravel.queue.attempt' => $this->toInteger($payload['attempts'] ?? null),
            'laravel.queue.delay_s' => $this->toInteger($event->delay),
            'laravel.queue.created_at_unix' => $this->toInteger($payload['createdAt'] ?? null),
        ]);
    }

    /**
     * Handle a processing event.
     */
    protected function handleProcessing(JobProcessing $event): void
    {
        $job = $this->jobContext($event->job);
        $key = $this->processingKey($job['job.uuid'], $job['job.id']);

        if ($key !== null) {
            $this->processingStartedAt[$key] = microtime(true);
        }

        $this->emit([
            ...$this->baseRecord('job.processing', $event->connectionName, $job['queue']),
            'messaging.operation.name' => 'process',
            ...$this->jobRecord($job),
            'laravel.queue.wait_ms' => $this->waitMilliseconds($event->job),
        ]);
    }

    /**
     * Handle a processed event.
     */
    protected function handleProcessed(JobProcessed $event): void
    {
        $job = $this->jobContext($event->job);
        $key = $this->processingKey($job['job.uuid'], $job['job.id']);

        $this->emit([
            ...$this->baseRecord('job.processed', $event->connectionName, $job['queue']),
            'messaging.operation.name' => 'process',
            ...$this->jobRecord($job),
            'laravel.queue.wait_ms' => $this->waitMilliseconds($event->job),
            'laravel.queue.duration_ms' => $this->durationMilliseconds($key),
            'laravel.queue.result' => $this->call($event->job, 'isDeleted') ? 'deleted' : 'processed',
        ]);

        $this->forgetDuration($key);
    }

    /**
     * Handle a released event.
     */
    protected function handleReleased(JobReleasedAfterException $event): void
    {
        $job = $this->jobContext($event->job);
        $key = $this->processingKey($job['job.uuid'], $job['job.id']);

        $this->emit([
            ...$this->baseRecord('job.released', $event->connectionName, $job['queue']),
            'messaging.operation.name' => 'settle',
            ...$this->jobRecord($job),
            'laravel.queue.backoff_s' => $this->toInteger($event->backoff),
            'laravel.queue.result' => 'released',
        ]);

        $this->forgetDuration($key);
    }

    /**
     * Handle a failed event.
     */
    protected function handleFailed(JobFailed $event): void
    {
        $job = $this->jobContext($event->job);
        $key = $this->processingKey($job['job.uuid'], $job['job.id']);

        $this->emit([
            ...$this->baseRecord('job.failed', $event->connectionName, $job['queue']),
            'messaging.operation.name' => 'settle',
            ...$this->jobRecord($job),
            'laravel.queue.result' => 'failed',
            'error.type' => $event->exception::class,
            'error.message' => $this->truncateErrorMessage($event->exception->getMessage()),
        ]);

        $this->forgetDuration($key);
    }

    /**
     * Handle a timed out event.
     */
    protected function handleTimedOut(JobTimedOut $event): void
    {
        $job = $this->jobContext($event->job);
        $key = $this->processingKey($job['job.uuid'], $job['job.id']);

        $this->emit([
            ...$this->baseRecord('job.timed_out', $event->connectionName, $job['queue']),
            'messaging.operation.name' => 'process',
            ...$this->jobRecord($job),
            'laravel.queue.result' => 'timed_out',
        ]);

        $this->forgetDuration($key);
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
            'service.name' => (string) $this->app['config']->get('app.name', 'laravel'),
            'deployment.environment.name' => (string) $this->app['config']->get('app.env', 'production'),
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
     * Get the queue event messaging system.
     */
    protected function messagingSystem(string $connectionName): string
    {
        return $this->toString(
            $this->app['config']->get("queue.connections.{$connectionName}.driver")
        ) ?? $connectionName;
    }

    /**
     * Get the current timestamp.
     */
    protected function timestamp(): string
    {
        return (new DateTimeImmutable('now', new DateTimeZone('UTC')))
            ->format('Y-m-d\TH:i:s.u\Z');
    }

    /**
     * Emit a queue event.
     *
     * @param  array<string, mixed>  $record
     * @return void
     */
    protected function emit(array $record): void
    {
        $payload = json_encode($record, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_INVALID_UTF8_SUBSTITUTE);

        if ($payload === false) {
            return;
        }

        if (is_callable(static::$writeUsing)) {
            (static::$writeUsing)($payload);

            return;
        }

        fwrite(STDOUT, $payload.PHP_EOL);
    }

    /**
     * Execute a callback safely.
     */
    protected function safely(callable $callback): void
    {
        try {
            $callback();
        } catch (Throwable) {
            // Ignore queue emission errors so queue processing can continue.
        }
    }

    /**
     * Decode a queue payload.
     *
     * @return array<string, mixed>
     */
    protected function decodePayload($payload): array
    {
        if (! is_string($payload) || $payload === '') {
            return [];
        }

        $decoded = json_decode($payload, true);

        return is_array($decoded) ? $decoded : [];
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

        if (! is_null($jobId) && $jobId !== '') {
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

        if (! is_array($payload)) {
            return null;
        }

        $createdAt = $this->toInteger($payload['createdAt'] ?? null);

        if ($createdAt === null) {
            return null;
        }

        return max((int) round((microtime(true) - $createdAt) * 1000), 0);
    }

    /**
     * Get the queue duration in milliseconds.
     */
    protected function durationMilliseconds(?string $key): ?int
    {
        if ($key === null || ! isset($this->processingStartedAt[$key])) {
            return null;
        }

        return max((int) round((microtime(true) - $this->processingStartedAt[$key]) * 1000), 0);
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
        if (is_string($job) && $job !== '') {
            return $job;
        }

        if ($job instanceof \Closure) {
            return 'Closure';
        }

        if (is_object($job)) {
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

        if (mb_strlen($message) <= static::ERROR_MESSAGE_LIMIT) {
            return $message;
        }

        return mb_substr($message, 0, static::ERROR_MESSAGE_LIMIT);
    }

    /**
     * Convert a value to a string.
     */
    protected function toString($value): ?string
    {
        if (! is_string($value) || $value === '') {
            return null;
        }

        return $value;
    }

    /**
     * Convert a value to an integer.
     */
    protected function toInteger($value): ?int
    {
        if (! is_numeric($value)) {
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
        return is_string($value) || is_int($value)
            ? $value
            : null;
    }

    /**
     * Call the given method on the target if possible.
     */
    protected function call($target, string $method)
    {
        if (! is_object($target) || ! method_exists($target, $method)) {
            return null;
        }

        try {
            return $target->{$method}();
        } catch (Throwable) {
            return null;
        }
    }
}

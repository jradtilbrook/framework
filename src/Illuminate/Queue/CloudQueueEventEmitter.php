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
    protected ?float $processingStartedAt = null;

    /**
     * The Laravel Cloud socket instance.
     *
     * @var \Illuminate\Foundation\LaravelCloudSocket
     */
    protected LaravelCloudSocket $socket;

    /**
     * Create a new event emitter instance.
     *
     * @param  \Illuminate\Foundation\LaravelCloudSocket  $socket
     */
    public function __construct(LaravelCloudSocket $socket)
    {
        $this->socket = $socket;
    }

    /**
     * Emit a queued job event.
     */
    public function queued(string $queue, float $delay): void
    {
        $this->emit([
            'type' => 'job.queued',
            'timestamp' => $this->timestamp(),
            'queue' => $queue,
            'delay' => $delay,
        ]);
    }

    /**
     * Emit a job processing event and begin duration timing.
     */
    public function processing(string $queue, ?float $wait): void
    {
        $this->processingStartedAt = microtime(true);

        $this->emit([
            'type' => 'job.processing',
            'timestamp' => $this->timestamp(),
            'queue' => $queue,
            'wait' => $wait,
        ]);
    }

    /**
     * Emit a job processed event and stop duration timing.
     */
    public function processed(string $queue): void
    {
        $duration = $this->durationSeconds();

        $this->emit([
            'type' => 'job.processed',
            'timestamp' => $this->timestamp(),
            'queue' => $queue,
            'duration' => $duration,
        ]);

        $this->forgetDuration();
    }

    /**
     * Emit a job released event and stop duration timing.
     */
    public function released(string $queue, int $backoff): void
    {
        $duration = $this->durationSeconds();

        $this->emit([
            'type' => 'job.released',
            'timestamp' => $this->timestamp(),
            'queue' => $queue,
            'backoff' => $backoff,
            'duration' => $duration,
        ]);

        $this->forgetDuration();
    }

    /**
     * Emit a job failed event and stop duration timing.
     */
    public function failed(string $queue): void
    {
        $duration = $this->durationSeconds();

        $this->emit([
            'type' => 'job.failed',
            'timestamp' => $this->timestamp(),
            'queue' => $queue,
            'duration' => $duration,
        ]);

        $this->forgetDuration();
    }

    /**
     * Get the current timestamp in seconds.
     */
    protected function timestamp(): float
    {
        return microtime(true);
    }

    /**
     * Get the queue duration in seconds.
     */
    protected function durationSeconds(): ?float
    {
        if ($this->processingStartedAt === null) {
            return null;
        }

        return max(microtime(true) - $this->processingStartedAt, 0.0);
    }

    /**
     * Forget a queue duration start time.
     */
    protected function forgetDuration(): void
    {
        $this->processingStartedAt = null;
    }

    /**
     * Emit a queue event.
     */
    protected function emit(array $record): void
    {
        $this->socket->writeJson($record);
    }
}

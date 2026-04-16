<?php

namespace Illuminate\Queue\Failed;

use Illuminate\Foundation\LaravelCloudSocket;
use Illuminate\Support\Facades\Date;

class LaravelCloudFailedJobProvider implements FailedJobProviderInterface
{
    /**
     * The Laravel Cloud socket instance.
     *
     * @var \Illuminate\Foundation\LaravelCloudSocket
     */
    protected LaravelCloudSocket $socket;

    /**
     * Create a new failed job provider instance.
     *
     * @param  \Illuminate\Foundation\LaravelCloudSocket  $socket
     */
    public function __construct(LaravelCloudSocket $socket)
    {
        $this->socket = $socket;
    }

    /**
     * Log a failed job into storage.
     *
     * @param  string  $connection
     * @param  string  $queue
     * @param  string  $payload
     * @param  \Throwable  $exception
     * @return int|null
     */
    public function log($connection, $queue, $payload, $exception)
    {
        $id = json_decode($payload, true)['uuid'] ?? null;

        $this->socket->writeJson([
            'id' => $id,
            'connection' => $connection,
            'queue' => $queue,
            'payload' => $payload,
            'exception' => (string) $exception,
            'failed_at' => Date::now()->toIso8601String(),
        ]);

        return $id;
    }

    /**
     * Get the IDs of all of the failed jobs.
     *
     * @param  string|null  $queue
     * @return array
     */
    public function ids($queue = null)
    {
        // Not supported...
        return [];
    }

    /**
     * Get a list of all of the failed jobs.
     *
     * @return array
     */
    public function all()
    {
        // Not supported...
        return [];
    }

    /**
     * Get a single failed job.
     *
     * @param  mixed  $id
     * @return object|null
     */
    public function find($id)
    {
        // Not supported...
        return null;
    }

    /**
     * Delete a single failed job from storage.
     *
     * @param  mixed  $id
     * @return bool
     */
    public function forget($id)
    {
        // Not supported...
        return false;
    }

    /**
     * Flush all of the failed jobs from storage.
     *
     * @param  int|null  $hours
     * @return void
     */
    public function flush($hours = null)
    {
        // Not supported...
    }
}

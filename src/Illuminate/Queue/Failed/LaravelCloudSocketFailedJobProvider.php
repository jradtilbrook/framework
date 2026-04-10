<?php

namespace Illuminate\Queue\Failed;

use Illuminate\Support\Facades\Date;

class LaravelCloudSocketFailedJobProvider implements FailedJobProviderInterface
{
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
        $id = json_decode($payload, true)['uuid'];

        $failedAt = Date::now();

        file_put_contents(
            $_ENV['LARAVEL_CLOUD_LOG_SOCKET'] ?? $_SERVER['LARAVEL_CLOUD_LOG_SOCKET'] ?? 'unix:///tmp/cloud-init.sock',
            [
                'id' => $id,
                'connection' => $connection,
                'queue' => $queue,
                'payload' => $payload,
                'exception' => (string) mb_convert_encoding($exception, 'UTF-8'),
                'failed_at' => $failedAt->format('Y-m-d H:i:s'),
                'failed_at_timestamp' => $failedAt->getTimestamp(),
            ]
        );

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

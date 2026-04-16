<?php

namespace Illuminate\Queue;

use Illuminate\Queue\Jobs\CloudJob;

class CloudQueue extends SqsQueue
{
    /**
     * Pop the next job off of the queue.
     *
     * @param  string  $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        $response = $this->sqs->receiveMessage([
            'QueueUrl' => $queue = $this->getQueue($queue),
        ]);

        if (! is_null($response['Messages']) && count($response['Messages']) > 0) {
            return tap(new CloudJob(
                $this->container, $this->sqs, $response['Messages'][0],
                $this->connectionName, $queue
            ), fn (CloudJob $job) => CloudQueueEventEmitter::processing($queue, now()->getTimestampMs() - $job->payload()['createdAtMs'])
            );
        }
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string  $payload
     * @param  string|null  $queue
     * @param  array  $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        return tap(parent::pushRaw($payload, $queue, $options),
            fn () => CloudQueueEventEmitter::queued($queue, (float) $options['DelaySeconds'] ?? 0)
        );

    }

    /**
     * Create a payload string from the given job and data.
     *
     * @param  string  $job
     * @param  string  $queue
     * @param  mixed  $data
     * @return array
     */
    protected function createPayloadArray($job, $queue, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $queue, $data), [
            'attempts' => 0,
            'createdAtMs' => now()->getTimestampMs(),
        ]);
    }
}

<?php

namespace Illuminate\Queue;

use Aws\Sqs\SqsClient;
use Illuminate\Queue\Jobs\CloudJob;

class CloudQueue extends SqsQueue
{
    /**
     * The event emitter instance.
     *
     * @var \Illuminate\Queue\CloudQueueEventEmitter
     */
    protected CloudQueueEventEmitter $emitter;

    /**
     * Create a new Amazon SQS queue instance.
     *
     * @param  \Aws\Sqs\SqsClient  $sqs
     * @param  \Illuminate\Queue\CloudQueueEventEmitter  $emitter
     * @param  string  $default
     * @param  string  $prefix
     * @param  string  $suffix
     * @param  bool  $dispatchAfterCommit
     */
    public function __construct(
        SqsClient $sqs,
        CloudQueueEventEmitter $emitter,
        $default,
        $prefix = '',
        $suffix = '',
        $dispatchAfterCommit = false,
    ) {
        parent::__construct($sqs, $default, $prefix, $suffix, $dispatchAfterCommit);
        $this->emitter = $emitter;
    }

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
                $this->connectionName, $queue, $this->emitter
            ), fn (CloudJob $job) => $this->emitter->processing($queue, now()->getTimestampMs() - $job->payload()['createdAtMs']));
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
            fn () => $this->emitter->queued($queue, (float) ($options['DelaySeconds'] ?? 0))
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

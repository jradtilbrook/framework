<?php

namespace Illuminate\Queue\Jobs;

use Aws\Sqs\SqsClient;
use Illuminate\Container\Container;
use Illuminate\Queue\CloudQueueEventEmitter;

class CloudJob extends SqsJob
{
    /**
     * The event emitter instance.
     *
     * @var \Illuminate\Queue\CloudQueueEventEmitter
     */
    protected CloudQueueEventEmitter $emitter;

    /**
     * Create a new job instance.
     *
     * @param  \Illuminate\Container\Container  $container
     * @param  \Aws\Sqs\SqsClient  $sqs
     * @param  array  $job
     * @param  string  $connectionName
     * @param  string  $queue
     * @param  \Illuminate\Queue\CloudQueueEventEmitter  $emitter
     */
    public function __construct(Container $container, SqsClient $sqs, array $job, $connectionName, $queue, CloudQueueEventEmitter $emitter)
    {
        parent::__construct($container, $sqs, $job, $connectionName, $queue);
        $this->emitter = $emitter;
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return ($this->payload()['attempts'] ?? 0) + 1;
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int  $delay
     * @return void
     */
    public function release($delay = 0)
    {
        $this->released = true;

        $payload = $this->payload();

        $payload['attempts'] = $this->attempts();

        $this->sqs->deleteMessage([
            'QueueUrl' => $this->queue,
            'ReceiptHandle' => $this->job['ReceiptHandle'],
        ]);

        $this->sqs->sendMessage([
            'QueueUrl' => $this->queue,
            'MessageBody' => json_encode($payload),
            'DelaySeconds' => $this->secondsUntil($delay),
        ]);

        $this->emitter->released($this->queue, $delay);
    }

    /**
     * Fire the job.
     *
     * @return void
     */
    public function fire()
    {
        parent::fire();

        $this->emitter->processed($this->queue);
    }

    /**
     * Delete the job, call the "failed" method, and raise the job failed event.
     *
     * @param  \Throwable|null  $e
     * @return void
     */
    public function fail($e = null)
    {
        parent::fail($e);

        $this->emitter->failed($this->queue);
    }
}

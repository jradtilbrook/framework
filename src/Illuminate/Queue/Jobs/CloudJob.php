<?php

namespace Illuminate\Queue\Jobs;

use Illuminate\Queue\CloudQueueEventEmitter;

class CloudJob extends SqsJob
{
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

        CloudQueueEventEmitter::released($this->queue, $delay);
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();

        $this->sqs->deleteMessage([
            'QueueUrl' => $this->queue, 'ReceiptHandle' => $this->job['ReceiptHandle'],
        ]);
    }

    /**
     * Fire the job.
     *
     * @return void
     */
    public function fire()
    {
        parent::fire();

        CloudQueueEventEmitter::processed($this->queue);
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

        CloudQueueEventEmitter::failed($this->queue);
    }
}

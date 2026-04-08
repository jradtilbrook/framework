<?php

namespace Illuminate\Tests\Integration\Foundation;

use Illuminate\Foundation\Cloud;
use Illuminate\Queue\CloudQueueEventEmitter;
use Illuminate\Support\Facades\Context;
use Orchestra\Testbench\TestCase;
use RuntimeException;

class CloudQueueEventEmitterTest extends TestCase
{
    protected function tearDown(): void
    {
        Context::flush();

        CloudQueueEventEmitter::writeUsing(null);
        CloudQueueEventEmitter::disable();

        parent::tearDown();
    }

    public function test_it_emits_queued_processing_and_processed_events()
    {
        $this->app['config']->set('app.name', 'framework-test');
        $this->app['config']->set('app.env', 'testing');
        $this->app['config']->set('queue.connections.redis.driver', 'redis');

        $records = [];

        CloudQueueEventEmitter::writeUsing(function ($payload) use (&$records) {
            $records[] = json_decode($payload, true);
        });

        Context::addHidden('laravel_cloud_request_id', 'req-abc-123');

        Cloud::configureQueueEventEmission($this->app);

        $createdAt = time() - 1;

        CloudQueueEventEmitter::queued(
            'redis',
            'default',
            '42',
            'App\\Jobs\\ShipOrder',
            json_encode([
                'uuid' => 'job-1',
                'displayName' => 'App\\Jobs\\ShipOrder',
                'createdAt' => $createdAt,
            ]),
            5,
        );

        $job = new CloudQueueEventEmitterFakeJob(
            id: '42',
            uuid: 'job-1',
            name: 'App\\Jobs\\ShipOrder',
            queue: 'default',
            attempts: 2,
            createdAt: $createdAt,
        );

        CloudQueueEventEmitter::processing('redis', $job);

        usleep(1000);

        CloudQueueEventEmitter::processed('redis', $job);

        $this->assertCount(3, $records);

        $queued = $records[0];
        $processing = $records[1];
        $processed = $records[2];

        $this->assertSame('1.0', $queued['schema_version']);
        $this->assertSame('laravel.queue.event', $queued['kind']);
        $this->assertSame('job.queued', $queued['event_name']);
        $this->assertSame('send', $queued['messaging.operation.name']);
        $this->assertSame('redis', $queued['messaging.system']);
        $this->assertSame('default', $queued['messaging.destination.name']);
        $this->assertSame('job-1', $queued['job.uuid']);
        $this->assertSame('42', $queued['job.id']);
        $this->assertSame('App\\Jobs\\ShipOrder', $queued['job.name']);
        $this->assertSame(5.0, $queued['laravel.queue.delay_s']);
        $this->assertSame($createdAt, $queued['laravel.queue.created_at_unix']);
        $this->assertSame('req-abc-123', $queued['correlation_id']);
        $this->assertArrayHasKey('service.name', $queued);
        $this->assertArrayHasKey('deployment.environment.name', $queued);

        $this->assertSame('job.processing', $processing['event_name']);
        $this->assertSame('process', $processing['messaging.operation.name']);
        $this->assertSame(2, $processing['laravel.queue.attempt']);
        $this->assertIsFloat($processing['laravel.queue.wait_s']);
        $this->assertSame('req-abc-123', $processing['correlation_id']);

        $this->assertSame('job.processed', $processed['event_name']);
        $this->assertSame('process', $processed['messaging.operation.name']);
        $this->assertSame('processed', $processed['laravel.queue.result']);
        $this->assertIsFloat($processed['laravel.queue.duration_s']);
        $this->assertSame('req-abc-123', $processed['correlation_id']);
    }

    public function test_it_emits_failed_event_with_a_truncated_error_message()
    {
        $records = [];

        CloudQueueEventEmitter::writeUsing(function ($payload) use (&$records) {
            $records[] = json_decode($payload, true);
        });

        Cloud::configureQueueEventEmission($this->app);

        $job = new CloudQueueEventEmitterFakeJob(
            id: '42',
            uuid: 'job-2',
            name: 'App\\Jobs\\ShipOrder',
            queue: 'default',
            attempts: 3,
            createdAt: time() - 1,
        );

        CloudQueueEventEmitter::failed('redis', $job, new RuntimeException(str_repeat('x', 600)));

        $this->assertCount(1, $records);

        $failed = $records[0];

        $this->assertSame('job.failed', $failed['event_name']);
        $this->assertSame('settle', $failed['messaging.operation.name']);
        $this->assertSame('failed', $failed['laravel.queue.result']);
        $this->assertSame(RuntimeException::class, $failed['error.type']);
        $this->assertSame(500, mb_strlen($failed['error.message']));
        $this->assertArrayNotHasKey('payload', $failed);
    }

    public function test_it_does_not_emit_queue_events_when_emitter_is_disabled()
    {
        $records = [];

        CloudQueueEventEmitter::writeUsing(function ($payload) use (&$records) {
            $records[] = json_decode($payload, true);
        });

        Cloud::configureQueueEventEmission($this->app);
        CloudQueueEventEmitter::disable();

        CloudQueueEventEmitter::queued(
            'redis',
            'default',
            '42',
            'App\\Jobs\\ShipOrder',
            json_encode(['uuid' => 'job-3', 'displayName' => 'App\\Jobs\\ShipOrder']),
            0,
        );

        $this->assertCount(0, $records);
    }

    public function test_it_does_not_emit_sync_jobs()
    {
        $this->app['config']->set('queue.connections.sync.driver', 'sync');

        $records = [];

        CloudQueueEventEmitter::writeUsing(function ($payload) use (&$records) {
            $records[] = json_decode($payload, true);
        });

        Cloud::configureQueueEventEmission($this->app);

        CloudQueueEventEmitter::queued(
            'sync',
            'default',
            '99',
            'App\\Jobs\\ShipOrder',
            json_encode(['uuid' => 'job-sync', 'displayName' => 'App\\Jobs\\ShipOrder']),
            0,
        );

        $this->assertCount(0, $records);
    }
}

class CloudQueueEventEmitterFakeJob
{
    public function __construct(
        public string $id,
        public ?string $uuid,
        public string $name,
        public string $queue,
        public int $attempts,
        public int $createdAt,
        public bool $deleted = false,
    ) {
    }

    public function getJobId(): string
    {
        return $this->id;
    }

    public function uuid(): ?string
    {
        return $this->uuid;
    }

    public function resolveName(): string
    {
        return $this->name;
    }

    public function getQueue(): string
    {
        return $this->queue;
    }

    public function attempts(): int
    {
        return $this->attempts;
    }

    public function payload(): array
    {
        return [
            'createdAt' => $this->createdAt,
        ];
    }

    public function isDeleted(): bool
    {
        return $this->deleted;
    }
}

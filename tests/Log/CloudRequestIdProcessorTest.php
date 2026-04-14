<?php

namespace Illuminate\Tests\Log;

use Illuminate\Container\Container;
use Illuminate\Http\Request;
use Illuminate\Log\CloudRequestIdProcessor;
use Monolog\Level;
use Monolog\LogRecord;
use PHPUnit\Framework\TestCase;

class CloudRequestIdProcessorTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        Container::setInstance(new Container);
    }

    protected function tearDown(): void
    {
        Container::setInstance(null);
        parent::tearDown();
    }

    protected function createRecord(): LogRecord
    {
        return new LogRecord(
            message: 'Test message',
            level: Level::Info,
            channel: 'test',
            datetime: new \DateTimeImmutable,
            extra: [],
            context: [],
        );
    }

    public function test_adds_cloud_request_id_from_header()
    {
        $app = Container::getInstance();
        $request = Request::create('/');
        $request->headers->set('X-Request-ID', 'test-request-id-123');
        $app->instance('request', $request);

        $processor = new CloudRequestIdProcessor;
        $record = $processor($this->createRecord());

        $this->assertEquals('test-request-id-123', $record->extra['cloud_request_id']);
    }

    public function test_does_not_add_field_when_no_request_bound()
    {
        $processor = new CloudRequestIdProcessor;
        $record = $processor($this->createRecord());

        $this->assertArrayNotHasKey('cloud_request_id', $record->extra);
    }

    public function test_does_not_add_field_when_no_header_present()
    {
        $app = Container::getInstance();
        $request = Request::create('/');
        $app->instance('request', $request);

        $processor = new CloudRequestIdProcessor;
        $record = $processor($this->createRecord());

        $this->assertArrayNotHasKey('cloud_request_id', $record->extra);
    }

    public function test_preserves_existing_extra_fields()
    {
        $app = Container::getInstance();
        $request = Request::create('/');
        $request->headers->set('X-Request-ID', 'test-id');
        $app->instance('request', $request);

        $record = new LogRecord(
            message: 'Test message',
            level: Level::Info,
            channel: 'test',
            datetime: new \DateTimeImmutable,
            extra: ['existing_field' => 'existing_value'],
            context: [],
        );

        $processor = new CloudRequestIdProcessor;
        $record = $processor($record);

        $this->assertEquals('existing_value', $record->extra['existing_field']);
        $this->assertEquals('test-id', $record->extra['cloud_request_id']);
    }
}

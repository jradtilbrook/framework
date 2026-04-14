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
        $request->headers->set('X-Request-ID', '550e8400-e29b-41d4-a716-446655440000');
        $app->instance('request', $request);

        $processor = new CloudRequestIdProcessor;
        $record = $processor($this->createRecord());

        $this->assertEquals('550e8400-e29b-41d4-a716-446655440000', $record->extra['cloud_request_id']);
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
        $request->headers->set('X-Request-ID', '6ba7b810-9dad-11d1-80b4-00c04fd430c8');
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
        $this->assertEquals('6ba7b810-9dad-11d1-80b4-00c04fd430c8', $record->extra['cloud_request_id']);
    }
}

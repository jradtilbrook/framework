<?php

namespace Illuminate\Tests\Integration\Log;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Context;
use Orchestra\Testbench\TestCase;

class CloudRequestIdContextTest extends TestCase
{
    protected $laravelCloud;

    protected $requestIdHeader;

    protected $requestId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->laravelCloud = $_SERVER['LARAVEL_CLOUD'] ?? null;
        $this->requestIdHeader = $_SERVER['LARAVEL_CLOUD_REQUEST_ID_HEADER'] ?? null;
        $this->requestId = $_SERVER['HTTP_X_REQUEST_ID'] ?? null;

        Context::flush();
    }

    protected function tearDown(): void
    {
        Context::flush();
        Context::clearResolvedInstance();

        if (is_null($this->laravelCloud)) {
            unset($_SERVER['LARAVEL_CLOUD']);
        } else {
            $_SERVER['LARAVEL_CLOUD'] = $this->laravelCloud;
        }

        if (is_null($this->requestIdHeader)) {
            unset($_SERVER['LARAVEL_CLOUD_REQUEST_ID_HEADER']);
        } else {
            $_SERVER['LARAVEL_CLOUD_REQUEST_ID_HEADER'] = $this->requestIdHeader;
        }

        if (is_null($this->requestId)) {
            unset($_SERVER['HTTP_X_REQUEST_ID']);
        } else {
            $_SERVER['HTTP_X_REQUEST_ID'] = $this->requestId;
        }

        parent::tearDown();
    }

    public function test_it_seeds_request_id_from_server_header_on_cloud()
    {
        $this->setCloudMode(true);
        $_SERVER['HTTP_X_REQUEST_ID'] = 'req-abc-123';

        $this->refreshContextRepository();

        $this->assertSame('req-abc-123', Context::getHidden('laravel_cloud_request_id'));
    }

    public function test_it_seeds_request_id_from_request_header_on_cloud()
    {
        $this->setCloudMode(true);
        unset($_SERVER['HTTP_X_REQUEST_ID']);

        $request = Request::create('/');
        $request->headers->set('X-Request-ID', 'req-abc-123');

        $this->app->instance('request', $request);

        $this->refreshContextRepository();

        $this->assertSame('req-abc-123', Context::getHidden('laravel_cloud_request_id'));
    }

    public function test_it_can_use_a_custom_request_id_header()
    {
        $this->setCloudMode(true);
        $_SERVER['LARAVEL_CLOUD_REQUEST_ID_HEADER'] = 'X-Laravel-Cloud-Request-Id';
        $_SERVER['HTTP_X_LARAVEL_CLOUD_REQUEST_ID'] = 'req-cloud-987';

        $this->refreshContextRepository();

        $this->assertSame('req-cloud-987', Context::getHidden('laravel_cloud_request_id'));

        unset($_SERVER['HTTP_X_LARAVEL_CLOUD_REQUEST_ID']);
    }

    public function test_it_does_not_seed_request_id_outside_cloud()
    {
        $this->setCloudMode(false);
        $_SERVER['HTTP_X_REQUEST_ID'] = 'req-abc-123';

        $this->refreshContextRepository();

        $this->assertNull(Context::getHidden('laravel_cloud_request_id'));
    }

    public function test_it_does_not_seed_request_id_without_header()
    {
        $this->setCloudMode(true);
        unset($_SERVER['HTTP_X_REQUEST_ID']);

        $this->refreshContextRepository();

        $this->assertNull(Context::getHidden('laravel_cloud_request_id'));
    }

    public function test_hidden_request_id_is_dehydrated_for_propagation()
    {
        $this->setCloudMode(true);
        $_SERVER['HTTP_X_REQUEST_ID'] = 'req-abc-123';

        $this->refreshContextRepository();

        $dehydrated = Context::dehydrate();

        $this->assertArrayHasKey('hidden', $dehydrated);
        $this->assertArrayHasKey('laravel_cloud_request_id', $dehydrated['hidden']);
        $this->assertSame('req-abc-123', unserialize($dehydrated['hidden']['laravel_cloud_request_id']));
    }

    protected function refreshContextRepository(): void
    {
        Context::flush();
        $this->app->forgetScopedInstances();
        Context::clearResolvedInstance();
    }

    protected function setCloudMode(bool $cloud): void
    {
        if ($cloud) {
            $_SERVER['LARAVEL_CLOUD'] = '1';
        } else {
            unset($_SERVER['LARAVEL_CLOUD']);
        }

        $this->refreshApplication();
        Context::clearResolvedInstance();
    }
}

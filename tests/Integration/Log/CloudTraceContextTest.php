<?php

namespace Illuminate\Tests\Integration\Log;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Context;
use Orchestra\Testbench\TestCase;
use Ramsey\Uuid\Uuid as BaseUuid;

class CloudTraceContextTest extends TestCase
{
    protected $laravelCloud;

    protected $cfRay;

    protected function setUp(): void
    {
        parent::setUp();

        $this->laravelCloud = $_SERVER['LARAVEL_CLOUD'] ?? null;
        $this->cfRay = $_SERVER['HTTP_CF_RAY'] ?? null;

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

        if (is_null($this->cfRay)) {
            unset($_SERVER['HTTP_CF_RAY']);
        } else {
            $_SERVER['HTTP_CF_RAY'] = $this->cfRay;
        }

        parent::tearDown();
    }

    public function test_it_seeds_trace_id_from_the_cf_ray_header_on_cloud()
    {
        $_SERVER['LARAVEL_CLOUD'] = '1';
        $_SERVER['HTTP_CF_RAY'] = 'abc123-SJC';

        $this->refreshContextRepository();

        $this->assertSame(
            str_replace('-', '', BaseUuid::uuid5('d7b1993d-1268-4f10-88e1-6fab60dbc51b', 'abc123-SJC')->toString()),
            Context::getHidden('laravel_cloud_trace_id')
        );
    }

    public function test_it_seeds_trace_id_from_request_header_on_cloud()
    {
        $_SERVER['LARAVEL_CLOUD'] = '1';
        unset($_SERVER['HTTP_CF_RAY']);

        $request = Request::create('/');
        $request->headers->set('CF-Ray', 'abc123-SJC');

        $this->app->instance('request', $request);

        $this->refreshContextRepository();

        $this->assertSame(
            str_replace('-', '', BaseUuid::uuid5('d7b1993d-1268-4f10-88e1-6fab60dbc51b', 'abc123-SJC')->toString()),
            Context::getHidden('laravel_cloud_trace_id')
        );
    }

    public function test_it_does_not_seed_trace_id_outside_cloud()
    {
        unset($_SERVER['LARAVEL_CLOUD']);
        $_SERVER['HTTP_CF_RAY'] = 'abc123-SJC';

        $this->refreshContextRepository();

        $this->assertNull(Context::getHidden('laravel_cloud_trace_id'));
    }

    public function test_it_does_not_seed_trace_id_without_cf_ray_header()
    {
        $_SERVER['LARAVEL_CLOUD'] = '1';
        unset($_SERVER['HTTP_CF_RAY']);

        $this->refreshContextRepository();

        $this->assertNull(Context::getHidden('laravel_cloud_trace_id'));
    }

    public function test_hidden_trace_id_is_dehydrated_for_propagation()
    {
        $_SERVER['LARAVEL_CLOUD'] = '1';
        $_SERVER['HTTP_CF_RAY'] = 'abc123-SJC';

        $this->refreshContextRepository();

        $dehydrated = Context::dehydrate();

        $this->assertArrayHasKey('hidden', $dehydrated);
        $this->assertArrayHasKey('laravel_cloud_trace_id', $dehydrated['hidden']);
        $this->assertSame(
            str_replace('-', '', BaseUuid::uuid5('d7b1993d-1268-4f10-88e1-6fab60dbc51b', 'abc123-SJC')->toString()),
            unserialize($dehydrated['hidden']['laravel_cloud_trace_id'])
        );
    }

    protected function refreshContextRepository(): void
    {
        Context::flush();
        $this->app->forgetScopedInstances();
        Context::clearResolvedInstance();
    }
}

<?php

namespace Illuminate\Log\Context;

use Illuminate\Contracts\Log\ContextLogProcessor as ContextLogProcessorContract;
use Illuminate\Http\Request;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Queue;
use Illuminate\Support\Env;
use Illuminate\Support\Facades\Context;
use Illuminate\Support\ServiceProvider;
use Ramsey\Uuid\Uuid as BaseUuid;

class ContextServiceProvider extends ServiceProvider
{
    /**
     * The namespace used for deterministic cloud trace IDs.
     *
     * @var string
     */
    protected const TRACE_SEED_NAMESPACE = 'd7b1993d-1268-4f10-88e1-6fab60dbc51b';

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $this->app->scoped(Repository::class);

        if ($this->app->runningInConsole()) {
            $this->app->resolving(Repository::class, function (Repository $repository) {
                $context = Env::get('__LARAVEL_CONTEXT');

                if ($context && $context = json_decode($context, associative: true)) {
                    $repository->hydrate($context);
                }
            });
        }

        $this->app->resolving(Repository::class, fn (Repository $repository) => $this->seedCloudTraceIdFromCfRay($repository));

        $this->app->bind(ContextLogProcessorContract::class, fn () => new ContextLogProcessor());
    }

    /**
     * Boot the application services.
     *
     * @return void
     */
    public function boot()
    {
        Queue::createPayloadUsing(function ($connection, $queue, $payload) {
            /** @phpstan-ignore staticMethod.notFound */
            $context = Context::dehydrate();

            return $context === null ? $payload : [
                ...$payload,
                'illuminate:log:context' => $context,
            ];
        });

        $this->app['events']->listen(function (JobProcessing $event) {
            /** @phpstan-ignore staticMethod.notFound */
            Context::hydrate($event->job->payload()['illuminate:log:context'] ?? null);
        });
    }

    /**
     * Seed cloud trace context values when running on Laravel Cloud.
     */
    protected function seedCloudTraceIdFromCfRay(Repository $repository): void
    {
        if (! laravel_cloud() || $repository->hasHidden('laravel_cloud_trace_id')) {
            return;
        }

        $cfRay = null;

        if ($this->app->bound('request') && $this->app['request'] instanceof Request) {
            $cfRay = $this->app['request']->header('CF-Ray');
        }

        if (! is_string($cfRay) || $cfRay === '') {
            $cfRay = $_SERVER['HTTP_CF_RAY'] ?? null;
        }

        if (! is_string($cfRay) || $cfRay === '') {
            return;
        }

        $repository->addHidden(
            'laravel_cloud_trace_id',
            str_replace('-', '', BaseUuid::uuid5(static::TRACE_SEED_NAMESPACE, $cfRay)->toString())
        );
    }
}

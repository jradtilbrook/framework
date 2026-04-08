<?php

namespace Illuminate\Log\Context;

use Illuminate\Contracts\Log\ContextLogProcessor as ContextLogProcessorContract;
use Illuminate\Http\Request;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Queue;
use Illuminate\Support\Env;
use Illuminate\Support\Facades\Context;
use Illuminate\Support\ServiceProvider;

class ContextServiceProvider extends ServiceProvider
{
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

        if (laravel_cloud()) {
            $this->app->resolving(Repository::class, fn (Repository $repository) => $this->addCloudRequestIdToContext($repository));
        }

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
     * Add cloud request ID from incoming headers.
     */
    protected function addCloudRequestIdToContext(Repository $repository): void
    {
        if ($repository->hasHidden('laravel_cloud_request_id')) {
            return;
        }

        $header = Env::get('LARAVEL_CLOUD_REQUEST_ID_HEADER', 'X-Request-ID');

        if (! is_string($header) || $header === '') {
            return;
        }

        $requestId = $this->requestHeader($header);

        if (! is_string($requestId) || $requestId === '') {
            return;
        }

        $repository->addHidden('laravel_cloud_request_id', $requestId);
    }

    /**
     * Resolve a request header value.
     */
    protected function requestHeader(string $name): ?string
    {
        if ($this->app->bound('request') && $this->app['request'] instanceof Request) {
            $value = $this->app['request']->header($name);

            if (is_string($value) && $value !== '') {
                return $value;
            }
        }

        $key = 'HTTP_'.strtoupper(str_replace('-', '_', $name));

        $value = $_SERVER[$key] ?? null;

        return is_string($value) && $value !== '' ? $value : null;
    }
}

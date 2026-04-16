<?php

namespace Illuminate\Queue\Connectors;

use Aws\Credentials\CredentialProvider;
use Aws\Sqs\SqsClient;
use Illuminate\Queue\CloudQueue;
use Illuminate\Queue\CloudQueueEventEmitter;
use Illuminate\Support\Arr;
use InvalidArgumentException;

class CloudConnector extends SqsConnector
{
    public function __construct(private CloudQueueEventEmitter $emitter)
    {
    }

    /**
     * Establish a queue connection.
     *
     * @param  array  $config
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        $config = $this->getDefaultConfiguration($config);

        $config['credentials'] = $this->resolveCredentials($config);

        return new CloudQueue(
            new SqsClient(
                Arr::except($config, ['token'])
            ),
            $this->emitter,
            $config['queue'],
            $config['prefix'] ?? '',
            $config['suffix'] ?? '',
            $config['after_commit'] ?? null
        );
    }

    /**
     * Resolve the credentials for the SQS connection.
     *
     * @param  array  $config
     * @return callable|null
     *
     * @throws \InvalidArgumentException
     */
    protected function resolveCredentials(array $config): ?callable
    {
        $credentials = $config['credentials'] ?? null;

        $provider = is_string($credentials) ? $credentials : ($credentials['provider'] ?? null);

        if (is_null($provider)) {
            return null;
        }

        $options = is_array($credentials) ? Arr::except($credentials, ['provider']) : [];

        return match ($provider) {
            'ecs' => CredentialProvider::ecsCredentials($options),
            'instance' => CredentialProvider::instanceProfile($options),
            default => throw new InvalidArgumentException("Invalid credential provider [{$provider}]."),
        };
    }
}

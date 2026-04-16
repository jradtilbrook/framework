<?php

namespace Illuminate\Queue\Connectors;

use Aws\Sqs\SqsClient;
use Illuminate\Foundation\LaravelCloudSocket;
use Illuminate\Queue\CloudQueue;
use Illuminate\Support\Arr;

class CloudConnector extends SqsConnector
{
    /**
     * Establish a queue connection.
     *
     * @param  array  $config
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        $config = $this->getDefaultConfiguration($config);

        if (! empty($config['key']) && ! empty($config['secret'])) {
            $config['credentials'] = Arr::only($config, ['key', 'secret']);

            if (! empty($config['token'])) {
                $config['credentials']['token'] = $config['token'];
            }
        }

        return new CloudQueue(
            new SqsClient(
                Arr::except($config, ['token'])
            ),
            app(LaravelCloudSocket::class),
            $config['queue'],
            $config['prefix'] ?? '',
            $config['suffix'] ?? '',
            $config['after_commit'] ?? null
        );
    }
}

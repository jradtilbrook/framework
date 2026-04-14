<?php

namespace Illuminate\Log;

use Illuminate\Container\Container;
use Monolog\LogRecord;
use Monolog\Processor\ProcessorInterface;

class CloudRequestIdProcessor implements ProcessorInterface
{
    /**
     * Add the cloud request ID to the log record.
     *
     * @param  \Monolog\LogRecord  $record
     * @return \Monolog\LogRecord
     */
    public function __invoke(LogRecord $record): LogRecord
    {
        $app = Container::getInstance();

        if (! $app->bound('request')) {
            return $record;
        }

        $request = $app->make('request');
        $requestId = $request->header('X-Request-ID');

        if ($requestId === null) {
            return $record;
        }

        return $record->with(context: [
            ...$record->context,
            'cloud_request_id' => $requestId,
        ]);
    }
}

<?php

namespace Illuminate\Queue;

class LaravelCloudSocket
{
    /**
     * The socket stream resource.
     *
     * @var resource|null
     */
    protected static $socket;

    /**
     * Write JSON data to the Laravel Cloud socket.
     *
     * @param  array  $data  The data to encode and write
     * @return bool
     */
    public static function writeJson(array $data): bool
    {
        $socketPath = $_ENV['LARAVEL_CLOUD_SOCKET'] ?? $_SERVER['LARAVEL_CLOUD_SOCKET'] ?? null;

        if (! is_string($socketPath) || $socketPath === '') {
            return false;
        }

        $payload = json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

        if ($payload === false) {
            return false;
        }

        return static::write($payload.PHP_EOL, $socketPath);
    }

    /**
     * Write raw data to the socket.
     *
     * @param  string  $payload  The data to write
     * @param  string  $socketPath
     * @return bool
     */
    protected static function write(string $payload, string $socketPath): bool
    {
        $socket = static::connect($socketPath);

        if (! static::isConnected($socket)) {
            return false;
        }

        if (@fwrite($socket, $payload) === false || @feof($socket)) {
            @fclose($socket);

            static::$socket = null;

            return false;
        }

        return true;
    }

    /**
     * Check if the socket is connected and healthy.
     *
     * @param  resource|null  $socket
     * @return bool
     */
    protected static function isConnected($socket): bool
    {
        return is_resource($socket) && ! @feof($socket);
    }

    /**
     * Get a connected socket resource.
     *
     * @param  string  $socketPath
     * @return resource|null
     */
    protected static function connect(string $socketPath)
    {
        if (static::isConnected(static::$socket)) {
            return static::$socket;
        }

        // Clear any stale socket reference
        if (is_resource(static::$socket)) {
            @fclose(static::$socket);
        }

        $socket = @stream_socket_client(
            $socketPath,
            $errorCode,
            $errorMessage,
            0.2,
            STREAM_CLIENT_CONNECT | STREAM_CLIENT_PERSISTENT,
        );

        if (! is_resource($socket)) {
            return static::$socket = null;
        }

        @stream_set_blocking($socket, false);

        return static::$socket = $socket;
    }

    /**
     * Close the socket.
     *
     * Note: Persistent sockets may remain open across PHP requests,
     * so this is primarily useful for cleanup within long-running processes.
     *
     * @return void
     */
    public static function close(): void
    {
        if (is_resource(static::$socket)) {
            @fclose(static::$socket);
            static::$socket = null;
        }
    }
}

<?php
/**
 * This file is part of the Cache library
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * @copyright Copyright (c) Ronam Unstirred (unforge.coder@gmail.com)
 * @license http://opensource.org/licenses/MIT MIT
 */

namespace Unforge\Toolkit\Cache;

use Unforge\Toolkit\Arr;
use Unforge\Toolkit\Logger; // todo
use Unforge\Abstraction\Cache\AbstractCache;
use Tarantool\Client\Client;
use Tarantool\Client\Connection\StreamConnection;
use Tarantool\Client\Packer\PurePacker;

/**
 * Class Tarantool
 * @package Unforge\Toolkit\Cache
 */
class Tarantool extends AbstractCache
{
    /**
     * @var Client
     */
    private $client;

    /**
     * @param array $config
     *
     * @throws \Exception
     */
    public function connect(array $config)
    {
        if (!extension_loaded('tarantool')) {
            throw new \Exception("Extension tarantool.so not installed");
        }

        $url        = Arr::getString($config, null);
        $options    = Arr::getArray($config, 'port', null);

        try {
            $conn = new StreamConnection($url, $options);
            $this->client = new Client($conn, new PurePacker());
        } catch (\Exception $e) {
            throw new \Exception($e->getMessage());
        }
    }

    /**
     * @param string $key
     * @param string $value
     * @param string $prefix
     *
     * @return bool
     */
    public function set(string $key, string $value, string $prefix = 'cache'): bool
    {
        $key = $this->prepareKeyToString($key);

        try {
            return (bool)$this->client->getSpace($prefix)->insert([$key, $value])->getSync();
        } catch (\Exception $e) {
            // todo Logger
            return false;
        }
    }

    /**
     * @param string $key
     * @param string $prefix
     *
     * @return string
     */
    public function get(string $key, string $prefix = 'cache'): string
    {
        $key = $this->prepareKeyToString($key);

        try {
            return (bool)$this->client->getSpace($prefix)->select([$key])->getData();
        } catch (\Exception $e) {
            // todo Logger
            return '';
        }
    }

    /**
     * @param string $key
     * @param string $prefix
     *
     * @return bool
     */
    public function del(string $key, string $prefix = 'cache'): bool
    {
        $key = $this->prepareKeyToString($key);

        try {
            return (bool)$this->client->getSpace($prefix)->delete([$key])->getSync();
        } catch (\Exception $e) {
            // todo Logger
            return false;
        }
    }

    /**
     * @param string $prefix
     *
     * @return bool
     */
    public function flush(string $prefix = 'cache'): bool
    {
        // TODO: Implement connect() method.
    }

    /**
     * @param string $key
     *
     * @return string
     */
    protected function prepareKeyToString(string $key): string
    {
        return str_replace("//", "_", $key) . "_" . md5($key);
    }
}

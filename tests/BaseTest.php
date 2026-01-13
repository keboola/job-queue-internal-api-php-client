<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Exception;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

abstract class BaseTest extends TestCase
{
    use TestEnvVarsTrait;
    public function setUp(): void
    {
        parent::setUp();

        $client = new Client([
            'url' => self::getRequiredEnv('TEST_STORAGE_API_URL'),
            'token' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
        ]);
        $tokenInfo = $client->verifyToken();
        self::assertIsScalar($tokenInfo['id']);
        self::assertIsScalar($tokenInfo['description']);
        self::assertIsArray($tokenInfo['owner']);
        self::assertIsScalar($tokenInfo['owner']['name']);
        self::assertIsScalar($tokenInfo['owner']['id']);

        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $client->getApiUrl(),
        ));
    }
}

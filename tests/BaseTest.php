<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Exception;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

abstract class BaseTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $client = new Client(
            ['url' => getenv('TEST_STORAGE_API_URL'), 'token' => getenv('TEST_STORAGE_API_TOKEN')],
        );
        $tokenInfo = $client->verifyToken();
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

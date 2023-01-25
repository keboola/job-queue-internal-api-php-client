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
        $requiredEnvs = ['TEST_QUEUE_API_URL', 'TEST_QUEUE_API_TOKEN',
            'TEST_STORAGE_API_URL', 'TEST_STORAGE_API_TOKEN', 'TEST_STORAGE_API_TOKEN_MASTER',
            'TEST_KMS_KEY_ID', 'TEST_KMS_REGION', 'TEST_AWS_ACCESS_KEY_ID', 'TEST_AWS_SECRET_ACCESS_KEY',
            'TEST_AZURE_CLIENT_ID', 'TEST_AZURE_CLIENT_SECRET', 'TEST_AZURE_TENANT_ID', 'TEST_AZURE_KEY_VAULT_URL'];
        foreach ($requiredEnvs as $env) {
            if (empty(getenv($env))) {
                throw new Exception(sprintf('Environment variable "%s" is empty', $env));
            }
        }
        $client = new Client(
            ['url' => getenv('TEST_STORAGE_API_URL'), 'token' => getenv('TEST_STORAGE_API_TOKEN')]
        );
        $tokenInfo = $client->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $client->getApiUrl()
        ));
    }
}

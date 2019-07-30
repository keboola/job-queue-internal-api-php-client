<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Exception;
use PHPUnit\Framework\TestCase;

abstract class BaseTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $requiredEnvs = ['TEST_QUEUE_API_URL', 'TEST_STORAGE_API_URL', 'TEST_STORAGE_API_TOKEN', 'TEST_KMS_KEY_ALIAS',
            'TEST_KMS_REGION', 'TEST_AWS_ACCESS_KEY_ID', 'TEST_AWS_SECRET_ACCESS_KEY'];
        foreach ($requiredEnvs as $env) {
            if (empty(getenv($env))) {
                throw new Exception(sprintf('Environment variable "%s" is empty', $env));
            }
        }
    }
}

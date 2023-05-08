<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\ObjectEncryptor\EncryptorOptions;

trait EncryptorOptionsTest
{
    use TestEnvVarsTrait;

    protected function getEncryptorOptions(): EncryptorOptions
    {
        $stackId = (string) parse_url(self::getRequiredEnv('TEST_STORAGE_API_URL'), PHP_URL_HOST);
        self::assertNotEmpty($stackId);

        return new EncryptorOptions(
            $stackId,
            self::getOptionalEnv('TEST_KMS_KEY_ID'),
            self::getOptionalEnv('TEST_KMS_REGION'),
            null,
            self::getOptionalEnv('TEST_AZURE_KEY_VAULT_URL'),
        );
    }
}

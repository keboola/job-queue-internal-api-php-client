<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\DataPlane\Config;

use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;

class KubernetesConfigTest extends TestCase
{
    public function testCreateAndGetData(): void
    {
        $config = new KubernetesConfig(
            'apiUrl',
            'token',
            'cert',
            'namespace'
        );

        self::assertSame('apiUrl', $config->getApiUrl());
        self::assertSame('token', $config->getToken());
        self::assertSame('cert', $config->getCertificateAuthority());
        self::assertSame('namespace', $config->getNamespace());
    }

    public function testGetDecryptedToken(): void
    {
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(new EncryptorOptions(
            (string) parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST),
            (string) getenv('TEST_KMS_KEY_ID'),
            (string) getenv('TEST_KMS_REGION'),
            null,
            (string) getenv('TEST_AZURE_KEY_VAULT_URL'),
        ));

        $encryptedToken = $objectEncryptor->encryptGeneric('tokenValue');
        self::assertStringStartsWith('KBC::Secure', $encryptedToken);

        $config = new KubernetesConfig(
            'apiUrl',
            $encryptedToken,
            'cert',
            'namespace'
        );

        $decryptedToken = $config->getTokenDecrypted($objectEncryptor);
        self::assertSame('tokenValue', $decryptedToken);
    }
}

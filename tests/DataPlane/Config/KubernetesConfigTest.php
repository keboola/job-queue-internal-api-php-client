<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\DataPlane\Config;

use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\Tests\EncryptorOptionsTest;
use Keboola\JobQueueInternalClient\Tests\TestEnvVarsTrait;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;

class KubernetesConfigTest extends TestCase
{
    use TestEnvVarsTrait;
    use EncryptorOptionsTest;

    public function setUp(): void
    {
        parent::setUp();
        putenv('AWS_ACCESS_KEY_ID=' . self::getRequiredEnv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . self::getRequiredEnv('TEST_AWS_SECRET_ACCESS_KEY'));
        putenv('AZURE_TENANT_ID=' . self::getRequiredEnv('TEST_AZURE_TENANT_ID'));
        putenv('AZURE_CLIENT_ID=' . self::getRequiredEnv('TEST_AZURE_CLIENT_ID'));
        putenv('AZURE_CLIENT_SECRET=' . self::getRequiredEnv('TEST_AZURE_CLIENT_SECRET'));
        putenv('GCP_KMS_KEY_ID=' . self::getRequiredEnv('TEST_GCP_KMS_KEY_ID'));
        putenv('GOOGLE_APPLICATION_CREDENTIALS=' . self::getRequiredEnv('TEST_GOOGLE_APPLICATION_CREDENTIALS'));
    }

    public function testCreateAndGetData(): void
    {
        $config = new KubernetesConfig(
            'apiUrl',
            'token',
            'cert',
            'namespace',
        );

        self::assertSame('apiUrl', $config->getApiUrl());
        self::assertSame('token', $config->getToken());
        self::assertSame('cert', $config->getCertificateAuthority());
        self::assertSame('namespace', $config->getNamespace());
    }

    public function testGetDecryptedToken(): void
    {
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(self::getEncryptorOptions());

        $encryptedToken = $objectEncryptor->encryptGeneric('tokenValue');
        self::assertStringStartsWith('KBC::Secure', $encryptedToken);

        $config = new KubernetesConfig(
            'apiUrl',
            $encryptedToken,
            'cert',
            'namespace',
        );

        $decryptedToken = $config->getTokenDecrypted($objectEncryptor);
        self::assertSame('tokenValue', $decryptedToken);
    }
}

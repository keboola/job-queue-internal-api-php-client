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
        $objectEncryptor = ObjectEncryptorFactory::getEncryptor(self::getEncryptorOptions());

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

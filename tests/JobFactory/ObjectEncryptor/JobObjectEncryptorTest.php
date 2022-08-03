<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\ObjectEncryptor;

use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use PHPUnit\Framework\TestCase;

class JobObjectEncryptorTest extends TestCase
{
    public function testEncrypt(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('encryptForProject')
            ->with('data', 'componentId', 'projectId')
            ->willReturn('encryptedData')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->encrypt('data', 'componentId', 'projectId');

        self::assertSame('encryptedData', $result);
    }

    public function testDecryptWithoutConfigId(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('decryptForProject')
            ->with('encryptedData', 'componentId', 'projectId')
            ->willReturn('data')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->decrypt('encryptedData', 'componentId', 'projectId', null);

        self::assertSame('data', $result);
    }

    public function testDecryptWithConfigId(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('decryptForConfiguration')
            ->with('encryptedData', 'componentId', 'projectId', 'configId')
            ->willReturn('data')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->decrypt('encryptedData', 'componentId', 'projectId', 'configId');

        self::assertSame('data', $result);
    }
}

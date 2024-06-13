<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\ObjectEncryptor;

use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\PermissionChecker\BranchType;
use PHPUnit\Framework\TestCase;

class JobObjectEncryptorTest extends TestCase
{
    public function testEncryptWithoutBranch(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('encryptForProject')
            ->with('data', 'componentId', 'projectId')
            ->willReturn('encryptedData')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->encrypt('data', 'componentId', 'projectId', null);

        self::assertSame('encryptedData', $result);
    }

    public function testEncryptWithBranch(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('encryptForBranchType')
            ->with('data', 'componentId', 'projectId', 'default')
            ->willReturn('encryptedData')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->encrypt('data', 'componentId', 'projectId', BranchType::DEFAULT);

        self::assertSame('encryptedData', $result);
    }

    public function testDecryptWithConfig(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('decryptForBranchTypeConfiguration')
            ->with('encryptedData', 'componentId', 'projectId', 'configId', 'dev')
            ->willReturn('data')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->decrypt('encryptedData', 'componentId', 'projectId', 'configId', BranchType::DEV);

        self::assertSame('data', $result);
    }

    public function testDecryptWithoutConfig(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('decryptForBranchType')
            ->with('encryptedData', 'componentId', 'projectId', 'default')
            ->willReturn('data')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);

        self::assertSame('data', $result);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\ObjectEncryptor;

use InvalidArgumentException;
use Keboola\JobQueueInternalClient\JobFactory\JobObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\PermissionChecker\BranchType;
use PHPUnit\Framework\TestCase;

class JobObjectEncryptorTest extends TestCase
{
    public function testEncryptWithoutBranchAndNoProtectedFeature(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('encryptForProject')
            ->with('data', 'componentId', 'projectId')
            ->willReturn('encryptedData')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->encrypt('data', 'componentId', 'projectId', null, []);

        self::assertSame('encryptedData', $result);
    }

    public function testEncryptWithBranchAndNoProtectedFeature(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('encryptForProject')
            ->with('data', 'componentId', 'projectId')
            ->willReturn('encryptedData')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->encrypt('data', 'componentId', 'projectId', BranchType::DEFAULT, []);

        self::assertSame('encryptedData', $result);
    }

    public function testEncryptWithProtectedDefaultBranchFeatureAndNoBranch(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::never())
            ->method('encryptForProject')
        ;
        $internalEncryptor->expects(self::never())
            ->method('encryptForBranchType')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Protected default branch feature is enabled, but branch type is not set.');

        $encryptor->encrypt('data', 'componentId', 'projectId', null, ['protected-default-branch']);
    }

    public function testEncryptWithProtectedDefaultBranchFeatureAndBranch(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('encryptForBranchType')
            ->with('data', 'componentId', 'projectId', 'default')
            ->willReturn('encryptedData')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->encrypt(
            'data',
            'componentId',
            'projectId',
            BranchType::DEFAULT,
            ['protected-default-branch'],
        );

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

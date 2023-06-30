<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\ObjectEncryptor;

use Generator;
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

    /** @dataProvider argumentsProvider */
    public function testDecrypt(array $arguments, array $expectedArguments, string $expectedMethod): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method($expectedMethod)
            ->with(...$expectedArguments)
            ->willReturn('data')
        ;

        $encryptor = new JobObjectEncryptor($internalEncryptor);
        $result = $encryptor->decrypt(...$arguments);

        self::assertSame('data', $result);
    }

    public function argumentsProvider(): Generator
    {
        yield 'without branch without configuration' => [
            'arguments' => ['encryptedData', 'componentId', 'projectId', null, null],
            'expectedArguments' => ['encryptedData', 'componentId', 'projectId'],
            'expectedMethod' => 'decryptForProject',
        ];
        yield 'with branch without configuration' => [
            'arguments' => ['encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT],
            'expectedArguments' => ['encryptedData', 'componentId', 'projectId', 'default'],
            'expectedMethod' => 'decryptForBranchType',
        ];
        yield 'without branch with configuration' => [
            'arguments' => ['encryptedData', 'componentId', 'projectId', 'configId', null],
            'expectedArguments' => ['encryptedData', 'componentId', 'projectId', 'configId'],
            'expectedMethod' => 'decryptForConfiguration',
        ];
        yield 'with branch with configuration' => [
            'arguments' => ['encryptedData', 'componentId', 'projectId', 'configId', BranchType::DEV],
            'expectedArguments' => ['encryptedData', 'componentId', 'projectId', 'configId', 'dev'],
            'expectedMethod' => 'decryptForBranchTypeConfiguration',
        ];
    }
}

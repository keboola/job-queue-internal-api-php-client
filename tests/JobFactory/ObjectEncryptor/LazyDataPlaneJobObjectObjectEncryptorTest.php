<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\ObjectEncryptor;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\TestingEncryptorConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\LazyDataPlaneJobObjectObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\PermissionChecker\BranchType;
use PHPUnit\Framework\TestCase;

class LazyDataPlaneJobObjectObjectEncryptorTest extends TestCase
{
    public function testEncrypt(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('encryptForProject')
            ->with('data', 'componentId', 'projectId')
            ->willReturn('encryptedData')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::once())
            ->method('fetchDataPlaneConfig')
            ->with('dataPlaneId')
            ->willReturn(new DataPlaneConfig(
                'dataPlaneId',
                new KubernetesConfig('', '', '', ''),
                new TestingEncryptorConfig($internalEncryptor)
            ))
        ;

        $encryptor = new LazyDataPlaneJobObjectObjectEncryptor($dataPlaneConfigRepository, 'dataPlaneId');
        $result = $encryptor->encrypt('data', 'componentId', 'projectId', null);

        self::assertSame('encryptedData', $result);
    }

    public function testDecryptWithoutConfigId(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('decryptForBranchType')
            ->with('encryptedData', 'componentId', 'projectId')
            ->willReturn('data')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::once())
            ->method('fetchDataPlaneConfig')
            ->with('dataPlaneId')
            ->willReturn(new DataPlaneConfig(
                'dataPlaneId',
                new KubernetesConfig('', '', '', ''),
                new TestingEncryptorConfig($internalEncryptor)
            ))
        ;

        $encryptor = new LazyDataPlaneJobObjectObjectEncryptor($dataPlaneConfigRepository, 'dataPlaneId');
        $result = $encryptor->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);

        self::assertSame('data', $result);
    }

    public function testDecryptWithConfigId(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor->expects(self::once())
            ->method('decryptForBranchTypeConfiguration')
            ->with('encryptedData', 'componentId', 'projectId', 'configId')
            ->willReturn('data')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::once())
            ->method('fetchDataPlaneConfig')
            ->with('dataPlaneId')
            ->willReturn(new DataPlaneConfig(
                'dataPlaneId',
                new KubernetesConfig('', '', '', ''),
                new TestingEncryptorConfig($internalEncryptor)
            ))
        ;

        $encryptor = new LazyDataPlaneJobObjectObjectEncryptor($dataPlaneConfigRepository, 'dataPlaneId');
        $result = $encryptor->decrypt('encryptedData', 'componentId', 'projectId', 'configId', BranchType::DEFAULT);

        self::assertSame('data', $result);
    }

    public function testDataPlaneConfigIsFetchedOnlyOnce(): void
    {
        $internalEncryptor = $this->createMock(ObjectEncryptor::class);
        $internalEncryptor
            ->method('encryptForProject')
            ->willReturn('encryptedData')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::once())
            ->method('fetchDataPlaneConfig')
            ->with('dataPlaneId')
            ->willReturn(new DataPlaneConfig(
                'dataPlaneId',
                new KubernetesConfig('', '', '', ''),
                new TestingEncryptorConfig($internalEncryptor)
            ))
        ;

        $encryptor = new LazyDataPlaneJobObjectObjectEncryptor($dataPlaneConfigRepository, 'dataPlaneId');

        $encryptor->encrypt('data', 'componentId', 'projectId', null);
        $encryptor->encrypt('other data', 'componentId', 'projectId', null);
        $encryptor->encrypt('data', 'componentId', 'otherProjectId', null);
        $encryptor->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);
        $encryptor->decrypt('encryptedData', 'componentId', 'projectId', 'configId', BranchType::DEFAULT);

        // no explicit assert is required, important is expects(self::once()) on DataPlaneConfigRepository
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\ObjectEncryptor;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\TestingEncryptorConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\LazyDataPlaneJobObjectObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptor;
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
        $result = $encryptor->decrypt('encryptedData', 'componentId', 'projectId', 'configId');

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

        $encryptor->encrypt('data', 'componentId', 'projectId');
        $encryptor->encrypt('other data', 'componentId', 'projectId');
        $encryptor->encrypt('data', 'componentId', 'otherProjectId');
        $encryptor->decrypt('encryptedData', 'componentId', 'projectId', null);
        $encryptor->decrypt('encryptedData', 'componentId', 'projectId', 'configId');

        // no explicit assert is required, important is expects(self::once()) on DataPlaneConfigRepository
    }
}

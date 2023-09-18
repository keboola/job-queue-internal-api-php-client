<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\AwsEncryptionConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\TestingEncryptorConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\LazyDataPlaneJobObjectObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\DataPlaneObjectEncryptorProvider;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\PermissionChecker\BranchType;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class DataPlaneObjectEncryptorProviderTest extends TestCase
{
    public function testGetEncryptorForProjectWithoutDataPlaneOnStackWithoutDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $controlPlaneObjectEncryptor->expects(self::once())
            ->method('decryptForBranchType')
            ->with('encryptedData', 'componentId', 'projectId', 'default')
            ->willReturn('data')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            false,
        );

        $result = $provider->getJobEncryptor([
            'id' => 'jobId',
            'projectId' => 'projectId',
            'dataPlaneId' => null,
        ]);

        // this should call decryptForProject and match the controlPlaneObjectEncryptor mock expectation
        $result->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);
    }

    public function testGetEncryptorForProjectWithDataPlaneOnStackWithDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $controlPlaneObjectEncryptor->expects(self::never())->method(self::anything());

        $dataPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $dataPlaneObjectEncryptor->expects(self::once())
            ->method('decryptForBranchType')
            ->with('encryptedData', 'componentId', 'projectId', 'default')
            ->willReturn('data')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::once())
            ->method('fetchDataPlaneConfig')
            ->with('dataPlaneId')
            ->willReturn(new DataPlaneConfig(
                'dataPlaneId',
                new KubernetesConfig('', '', '', ''),
                new TestingEncryptorConfig($dataPlaneObjectEncryptor),
            ))
        ;

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            true,
        );

        $result = $provider->getJobEncryptor([
            'id' => 'jobId',
            'projectId' => 'projectId',
            'dataPlaneId' => 'dataPlaneId',
        ]);

        self::assertInstanceOf(LazyDataPlaneJobObjectObjectEncryptor::class, $result);

        // this should call decryptForProject and match the dataPlaneObjectEncryptor mock expectation
        $x = $result->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);
        self::assertSame('data', $x);
    }

    public function testGetEncryptorForProjectWithoutDataPlaneOnStackWithDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $controlPlaneObjectEncryptor->expects(self::once())
            ->method('decryptForBranchType')
            ->with('encryptedData', 'componentId', 'projectId', 'default')
            ->willReturn('data')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            true,
        );

        $result = $provider->getJobEncryptor([
            'id' => 'jobId',
            'projectId' => 'projectId',
            'dataPlaneId' => null,
        ]);

        self::assertInstanceOf(JobObjectEncryptor::class, $result);

        // this should call decryptForProject and match the controlPlaneObjectEncryptor mock expectation
        $x = $result->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);
        self::assertSame('data', $x);
    }

    public function testGetEncryptorForProjectWithDataPlaneOnStackWithoutDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $controlPlaneObjectEncryptor->expects(self::never())->method(self::anything());

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            false,
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Can\'t provide dataPlane encryptor on stack without dataPlane support');

        $provider->getJobEncryptor([
            'id' => 'jobId',
            'projectId' => 'projectId',
            'dataPlaneId' => 'dataPlaneId',
        ]);
    }

    public function testResolveProjectDataPlaneConfigOnStackWithDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);

        $dataPlaneConfig = new DataPlaneConfig(
            'dataPlaneId',
            new KubernetesConfig('', '', '', ''),
            new AwsEncryptionConfig('stackId', 'kmsRegion', 'kmsId', null),
        );

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::once())
            ->method('fetchProjectDataPlane')
            ->with('projectId')
            ->willReturn($dataPlaneConfig)
        ;

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            true,
        );

        $result = $provider->resolveProjectDataPlaneConfig('projectId');

        self::assertSame($result, $dataPlaneConfig);
    }

    public function testResolveProjectDataPlaneConfigOnStackWithoutDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            false,
        );

        $result = $provider->resolveProjectDataPlaneConfig('projectId');

        self::assertNull($result);
    }

    public function testGetProjectObjectEncryptorForProjectWithoutDataPlaneOnStackWithoutDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $controlPlaneObjectEncryptor->expects(self::once())
            ->method('decryptForBranchType')
            ->with('encryptedData', 'componentId', 'projectId', 'default')
            ->willReturn('data')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            false,
        );

        $result = $provider->getProjectObjectEncryptor(null);

        // this should call decryptForProject and match the controlPlaneObjectEncryptor mock expectation
        $result->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);
    }

    public function testGetProjectObjectEncryptorForProjectWithDataPlaneOnStackWithDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $controlPlaneObjectEncryptor->expects(self::never())->method(self::anything());

        $dataPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $dataPlaneObjectEncryptor->expects(self::once())
            ->method('decryptForBranchType')
            ->with('encryptedData', 'componentId', 'projectId', 'default')
            ->willReturn('data')
        ;

        $dataPlaneConfig = new DataPlaneConfig(
            'dataPlaneId',
            new KubernetesConfig('', '', '', ''),
            new TestingEncryptorConfig($dataPlaneObjectEncryptor),
        );

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            true,
        );

        $result = $provider->getProjectObjectEncryptor($dataPlaneConfig);

        // this should call decryptForProject and match the dataPlaneObjectEncryptor mock expectation
        $x = $result->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);
        self::assertSame('data', $x);
    }

    public function testGetProjectObjectEncryptorForProjectWithoutDataPlaneOnStackWithDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $controlPlaneObjectEncryptor->expects(self::once())
            ->method('decryptForBranchType')
            ->with('encryptedData', 'componentId', 'projectId', 'default')
            ->willReturn('data')
        ;

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            true,
        );

        $result = $provider->getProjectObjectEncryptor(null);

        self::assertInstanceOf(JobObjectEncryptor::class, $result);

        // this should call decryptForProject and match the controlPlaneObjectEncryptor mock expectation
        $x = $result->decrypt('encryptedData', 'componentId', 'projectId', null, BranchType::DEFAULT);
        self::assertSame('data', $x);
    }

    public function testGetProjectObjectEncryptorForProjectWithDataPlaneOnStackWithoutDataPlanes(): void
    {
        $controlPlaneObjectEncryptor = $this->createMock(ObjectEncryptor::class);
        $controlPlaneObjectEncryptor->expects(self::never())->method(self::anything());

        $dataPlaneConfig = new DataPlaneConfig(
            'dataPlaneId',
            new KubernetesConfig('', '', '', ''),
            new AwsEncryptionConfig('stackId', 'kmsRegion', 'kmsKeyId', null),
        );

        $dataPlaneConfigRepository = $this->createMock(DataPlaneConfigRepository::class);
        $dataPlaneConfigRepository->expects(self::never())->method(self::anything());

        $provider = new DataPlaneObjectEncryptorProvider(
            $controlPlaneObjectEncryptor,
            $dataPlaneConfigRepository,
            false,
        );

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Can\'t provide dataPlane encryptor on stack without dataPlane support');

        $provider->getProjectObjectEncryptor($dataPlaneConfig);
    }
}

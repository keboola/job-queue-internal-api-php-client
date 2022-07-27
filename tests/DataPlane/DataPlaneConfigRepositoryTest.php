<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\DataPlane;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\AwsEncryptionConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigValidator;
use Keboola\JobQueueInternalClient\DataPlane\Exception\DataPlaneNotFoundException;
use Keboola\JobQueueInternalClient\DataPlane\Exception\InvalidDataPlaneConfigurationException;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Validator\Validation;

class DataPlaneConfigRepositoryTest extends TestCase
{
    private const DATA_PLANE_CONFIG = [
        'id' => 1,
        'parameters' => [
            'encryption' => [
                'type' => 'aws',
                'kmsKeyId' => 'kmsKeyId',
                'kmsRoleArn' => 'kmsRoleArn',
            ],
            'kubernetes' => [
                'apiUrl' => 'https://kubernetes.local',
                'token' => 'token',
                'certificateAuthority' => 'certificateAuthority',
                'namespace' => 'namespace',
            ],
        ],
    ];

    public function testFetchProjectDataPlaneOfProjectWithNoneDataPlane(): void
    {
        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient->expects(self::once())
            ->method('getProject')
            ->with('projectId')
            ->willReturn([
                'id' => 'projectId',
            ])
        ;

        $configValidator = $this->createMock(DataPlaneConfigValidator::class);
        $configValidator->expects(self::never())->method('validateDataPlaneConfig');

        $repository = new DataPlaneConfigRepository(
            $manageApiClient,
            $configValidator,
            'stackId',
            'kmsRegion'
        );

        $result = $repository->fetchProjectDataPlane('projectId');
        self::assertNull($result);
    }

    public function testFetchProjectDataPlaneOfProjectWithMultipleDataPlanes(): void
    {
        $dataPlane1 = self::DATA_PLANE_CONFIG;

        $dataPlane2 = self::DATA_PLANE_CONFIG;
        $dataPlane2['id'] = 2;

        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient->expects(self::once())
            ->method('getProject')
            ->with('projectId')
            ->willReturn([
                'id' => 'projectId',
                'dataPlanes' => [
                    $dataPlane1,
                    $dataPlane2,
                ],
            ])
        ;

        $configValidator = $this->createMock(DataPlaneConfigValidator::class);
        $configValidator->expects(self::once())
            ->method('validateDataPlaneConfig')
            ->with('1', $dataPlane1['parameters'])
            ->willReturnArgument(1)
        ;

        $repository = new DataPlaneConfigRepository(
            $manageApiClient,
            $configValidator,
            'stackId',
            'kmsRegion'
        );

        $result = $repository->fetchProjectDataPlane('projectId');
        self::assertNotNull($result);
        self::assertEquals(new DataPlaneConfig(
            '1',
            new KubernetesConfig(
                'https://kubernetes.local',
                'token',
                'certificateAuthority',
                'namespace',
            ),
            new AwsEncryptionConfig(
                'stackId',
                'kmsRegion',
                'kmsKeyId',
                'kmsRoleArn',
            ),
        ), $result);
    }

    public function testFetchProjectDataPlaneOfProjectWithInvalidDataPlane(): void
    {
        $dataPlane = self::DATA_PLANE_CONFIG;
        unset($dataPlane['parameters']['encryption']['type']);

        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient->expects(self::once())
            ->method('getProject')
            ->with('projectId')
            ->willReturn([
                'id' => 'projectId',
                'dataPlanes' => [
                    $dataPlane,
                ],
            ])
        ;

        $configValidator = new DataPlaneConfigValidator(Validation::createValidator());

        $repository = new DataPlaneConfigRepository(
            $manageApiClient,
            $configValidator,
            'stackId',
            'kmsRegion'
        );

        $this->expectException(InvalidDataPlaneConfigurationException::class);
        $this->expectExceptionMessage(
            'Data plane "1" configuration is not valid: [encryption][type] This field is missing.'
        );

        $repository->fetchProjectDataPlane('projectId');
    }

    public function testFetchDataPlaneConfig(): void
    {
        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient->expects(self::once())
            ->method('getDataPlane')
            ->with('1')
            ->willReturn(self::DATA_PLANE_CONFIG)
        ;

        $configValidator = $this->createMock(DataPlaneConfigValidator::class);
        $configValidator->expects(self::once())
            ->method('validateDataPlaneConfig')
            ->with('1', self::DATA_PLANE_CONFIG['parameters'])
            ->willReturnArgument(1)
        ;

        $repository = new DataPlaneConfigRepository(
            $manageApiClient,
            $configValidator,
            'stackId',
            'kmsRegion'
        );

        $result = $repository->fetchDataPlaneConfig('1');
        self::assertEquals(new DataPlaneConfig(
            '1',
            new KubernetesConfig(
                'https://kubernetes.local',
                'token',
                'certificateAuthority',
                'namespace',
            ),
            new AwsEncryptionConfig(
                'stackId',
                'kmsRegion',
                'kmsKeyId',
                'kmsRoleArn',
            ),
        ), $result);
    }

    public function testFetchDataPlaneConfigThatDoesNotExist(): void
    {
        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient->expects(self::once())
            ->method('getDataPlane')
            ->with('1')
            ->willThrowException(new ClientException('Data plane not found', 404))
        ;

        $configValidator = $this->createMock(DataPlaneConfigValidator::class);
        $configValidator->expects(self::never())->method('validateDataPlaneConfig');

        $repository = new DataPlaneConfigRepository(
            $manageApiClient,
            $configValidator,
            'stackId',
            'kmsRegion'
        );

        $this->expectException(DataPlaneNotFoundException::class);
        $this->expectExceptionMessage('Data plane "1" not found');

        $repository->fetchDataPlaneConfig('1');
    }

    public function testFetchDataPlaneConfigWithInvalidData(): void
    {
        $dataPlane = self::DATA_PLANE_CONFIG;
        unset($dataPlane['parameters']['encryption']['type']);

        $manageApiClient = $this->createMock(ManageApiClient::class);
        $manageApiClient->expects(self::once())
            ->method('getDataPlane')
            ->with('1')
            ->willReturn($dataPlane)
        ;

        $configValidator = new DataPlaneConfigValidator(Validation::createValidator());

        $repository = new DataPlaneConfigRepository(
            $manageApiClient,
            $configValidator,
            'stackId',
            'kmsRegion'
        );

        $this->expectException(InvalidDataPlaneConfigurationException::class);
        $this->expectExceptionMessage(
            'Data plane "1" configuration is not valid: [encryption][type] This field is missing.'
        );

        $repository->fetchDataPlaneConfig('1');
    }
}

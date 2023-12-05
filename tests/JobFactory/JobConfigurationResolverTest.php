<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\JobConfigurationResolver;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\StorageApi\Components;
use PHPUnit\Framework\TestCase;

class JobConfigurationResolverTest extends TestCase
{
    public function testResolveJobConfiguration(): void
    {
        $jobMock = $this->createMock(JobInterface::class);
        $jobMock->expects(self::exactly(2))
            ->method('getConfigId')
            ->willReturn('123')
        ;
        $jobMock->expects(self::once())
            ->method('getComponentId')
            ->willReturn('keboola.test-component')
        ;

        $componentsApiClientMock = $this->createMock(Components::class);
        $componentsApiClientMock->expects(self::once())
            ->method('getConfiguration')
            ->with('keboola.test-component', '123')
            ->willReturn(['id' => '123', 'name' => 'test'])
        ;

        self::assertSame(
            ['id' => '123', 'name' => 'test'],
            JobConfigurationResolver::resolveJobConfiguration($jobMock, $componentsApiClientMock),
        );
    }

    public function testResolveJobConfigurationFailsForNoConfigId(): void
    {
        $jobMock = $this->createMock(JobInterface::class);
        $jobMock->expects(self::once())
            ->method('getConfigId')
            ->willReturn(null)
        ;

        $this->expectExceptionMessage('Can\'t fetch component configuration: job has no configId set');
        JobConfigurationResolver::resolveJobConfiguration(
            $jobMock,
            $this->createMock(Components::class),
        );
    }

    public function testResolveJobConfigurationHandlesStorageApiErrors(): void
    {
        $jobMock = $this->createMock(JobInterface::class);
        $jobMock->expects(self::exactly(2))
            ->method('getConfigId')
            ->willReturn('123')
        ;
        $jobMock->expects(self::once())
            ->method('getComponentId')
            ->willReturn('keboola.test-component')
        ;

        $componentsApiClientMock = $this->createMock(Components::class);
        $componentsApiClientMock->expects(self::once())
            ->method('getConfiguration')
            ->with('keboola.test-component', '123')
            ->willThrowException(new StorageApiClientException('Sample error'))
        ;

        $this->expectExceptionMessage('Failed to fetch component configuration: Sample error');
        $this->expectException(ClientException::class);
        JobConfigurationResolver::resolveJobConfiguration($jobMock, $componentsApiClientMock);
    }
}

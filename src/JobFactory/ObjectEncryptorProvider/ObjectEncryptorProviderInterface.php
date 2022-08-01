<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;

interface ObjectEncryptorProviderInterface
{
    public function getProjectDataPlaneConfig(string $projectId): ?DataPlaneConfig;

    public function getExistingJobEncryptor(?string $dataPlaneId): JobObjectEncryptorInterface;

    public function getDataPlaneObjectEncryptor(?DataPlaneConfig $dataPlaneConfig): JobObjectEncryptor;
}

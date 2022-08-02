<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;

interface ObjectEncryptorProviderInterface
{
    public function getProjectObjectEncryptor(string $projectId): JobObjectEncryptorInterface;

    public function getExistingJobEncryptor(?string $dataPlaneId): JobObjectEncryptorInterface;
}

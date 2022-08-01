<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;
use Keboola\ObjectEncryptor\ObjectEncryptor;

class ConcreteObjectEncryptorProvider implements ObjectEncryptorProviderInterface
{
    private ObjectEncryptor $objectEncryptor;

    public function __construct(ObjectEncryptor $objectEncryptor)
    {
        $this->objectEncryptor = $objectEncryptor;
    }

    public function getProjectDataPlaneConfig(string $projectId): ?DataPlaneConfig
    {
        return null;
    }

    public function getExistingJobEncryptor(?string $dataPlaneId): JobObjectEncryptorInterface
    {
        return new JobObjectEncryptor($this->objectEncryptor);
    }

    public function getDataPlaneObjectEncryptor(?DataPlaneConfig $dataPlaneConfig): JobObjectEncryptor
    {
        return new JobObjectEncryptor($this->objectEncryptor);
    }
}

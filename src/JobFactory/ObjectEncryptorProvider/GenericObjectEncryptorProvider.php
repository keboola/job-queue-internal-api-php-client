<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\LazyDataPlaneJobObjectObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptor;

class GenericObjectEncryptorProvider implements ObjectEncryptorProviderInterface
{
    private ObjectEncryptor $objectEncryptor;

    public function __construct(ObjectEncryptor $objectEncryptor)
    {
        $this->objectEncryptor = $objectEncryptor;
    }

    public function getProjectObjectEncryptor(string $projectId): JobObjectEncryptorInterface
    {
        return new JobObjectEncryptor($this->objectEncryptor);
    }

    public function getExistingJobEncryptor(?string $dataPlaneId): JobObjectEncryptorInterface
    {
        return new JobObjectEncryptor($this->objectEncryptor);
    }
}

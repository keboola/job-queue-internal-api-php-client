<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\DataPlaneJobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\LazyDataPlaneJobObjectObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use RuntimeException;

class DataPlaneObjectEncryptorProvider implements ObjectEncryptorProviderInterface
{
    private ObjectEncryptor $controlPlaneObjectEncryptor;
    private DataPlaneConfigRepository $dataPlaneConfigRepository;
    private bool $supportsDataPlanes;

    public function __construct(
        ObjectEncryptor $controlPlaneObjectEncryptor,
        DataPlaneConfigRepository $dataPlaneConfigRepository,
        bool $supportsDataPlanes
    ) {
        $this->controlPlaneObjectEncryptor = $controlPlaneObjectEncryptor;
        $this->dataPlaneConfigRepository = $dataPlaneConfigRepository;
        $this->supportsDataPlanes = $supportsDataPlanes;
    }

    public function getProjectDataPlaneConfig(string $projectId): ?DataPlaneConfig
    {
        if (!$this->supportsDataPlanes) {
            return null;
        }

        return $this->dataPlaneConfigRepository->fetchProjectDataPlane($projectId);
    }

    public function getProjectObjectEncryptor(string $projectId): JobObjectEncryptorInterface
    {
        if (!$this->supportsDataPlanes) {
            return new JobObjectEncryptor($this->controlPlaneObjectEncryptor);
        }

        $dataPlaneConfig = $this->dataPlaneConfigRepository->fetchProjectDataPlane($projectId);

        if ($dataPlaneConfig === null) {
            return new JobObjectEncryptor($this->controlPlaneObjectEncryptor);
        }

        return new DataPlaneJobObjectEncryptor(
            $dataPlaneConfig->getId(),
            $dataPlaneConfig->getEncryption()->createEncryptor(),
        );
    }

    public function getExistingJobEncryptor(?string $dataPlaneId): JobObjectEncryptorInterface
    {
        return new LazyDataPlaneJobObjectObjectEncryptor(
            $this->dataPlaneConfigRepository,
            $dataPlaneId
        );
    }
}

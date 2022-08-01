<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\LazyDataPlaneJobObjectEncryptor;
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

    public function getExistingJobEncryptor(?string $dataPlaneId): LazyDataPlaneJobObjectEncryptor
    {
        return new LazyDataPlaneJobObjectEncryptor(
            $this->dataPlaneConfigRepository,
            $dataPlaneId
        );
    }

    public function getDataPlaneObjectEncryptor(?DataPlaneConfig $dataPlaneConfig): JobObjectEncryptor
    {
        if (!$this->supportsDataPlanes) {
            if ($dataPlaneConfig !== null) {
                throw new RuntimeException('ERROR');
            }

            return new JobObjectEncryptor($this->controlPlaneObjectEncryptor);
        }

        if ($dataPlaneConfig === null) {
            return new JobObjectEncryptor($this->controlPlaneObjectEncryptor);
        }

        return new JobObjectEncryptor($dataPlaneConfig->getEncryption()->createEncryptor());
    }
}

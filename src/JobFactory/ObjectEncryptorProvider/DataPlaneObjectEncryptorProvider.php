<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Exception;
use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
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

    public function getJobEncryptor(array $jobData): JobObjectEncryptorInterface
    {
        $dataPlaneId = $jobData['dataPlaneId'] ?? null;

        if (!$this->supportsDataPlanes) {
            if ($dataPlaneId !== null) {
                throw new RuntimeException('Can\'t provide dataPlane encryptor on stack without dataPlane support');
            }

            return new JobObjectEncryptor($this->controlPlaneObjectEncryptor);
        }

        return new LazyDataPlaneJobObjectObjectEncryptor(
            $this->dataPlaneConfigRepository,
            $dataPlaneId
        );
    }

    public function resolveProjectDataPlaneConfig(string $projectId): ?DataPlaneConfig
    {
        if (!$this->supportsDataPlanes) {
            return null;
        }

        return $this->dataPlaneConfigRepository->fetchProjectDataPlane($projectId);
    }

    public function getProjectObjectEncryptor(?DataPlaneConfig $dataPlaneConfig): JobObjectEncryptor
    {
        if (!$this->supportsDataPlanes) {
            if ($dataPlaneConfig !== null) {
                throw new RuntimeException('Can\'t provide dataPlane encryptor on stack without dataPlane support');
            }

            return new JobObjectEncryptor($this->controlPlaneObjectEncryptor);
        }

        if ($dataPlaneConfig === null) {
            return new JobObjectEncryptor($this->controlPlaneObjectEncryptor);
        }

        return new JobObjectEncryptor($dataPlaneConfig->getEncryption()->createEncryptor());
    }
}

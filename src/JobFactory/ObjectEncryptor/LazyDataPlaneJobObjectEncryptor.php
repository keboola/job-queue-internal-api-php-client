<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;

class LazyDataPlaneJobObjectEncryptor implements JobObjectEncryptorInterface
{
    private DataPlaneConfigRepository $dataPlaneConfigRepository;
    private string $dataPlaneId;

    private ?JobObjectEncryptor $dataPlaneObjectEncryptor;

    public function __construct(DataPlaneConfigRepository $dataPlaneConfigRepository, string $dataPlaneId)
    {
        $this->dataPlaneConfigRepository = $dataPlaneConfigRepository;
        $this->dataPlaneId = $dataPlaneId;
    }

    public function encrypt($data, string $componentId, string $projectId)
    {
        return $this->getEncryptor()->encrypt($data, $componentId, $projectId);
    }

    public function decrypt($data, string $componentId, string $projectId, ?string $configId)
    {
        return $this->getEncryptor()->decrypt($data, $componentId, $projectId, $configId);
    }

    private function getEncryptor(): JobObjectEncryptor
    {
        if ($this->dataPlaneObjectEncryptor !== null) {
            return $this->dataPlaneObjectEncryptor;
        }

        $objectEncryptor = $this->dataPlaneConfigRepository
            ->fetchDataPlaneConfig($this->dataPlaneId)
            ->getEncryption()
            ->createEncryptor()
        ;

        return $this->dataPlaneObjectEncryptor = new JobObjectEncryptor($objectEncryptor);
    }
}

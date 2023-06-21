<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\PermissionChecker\BranchType;

class LazyDataPlaneJobObjectObjectEncryptor implements JobObjectEncryptorInterface
{
    private DataPlaneConfigRepository $dataPlaneConfigRepository;
    private string $dataPlaneId;

    private ?JobObjectEncryptor $dataPlaneObjectEncryptor = null;

    public function __construct(DataPlaneConfigRepository $dataPlaneConfigRepository, string $dataPlaneId)
    {
        $this->dataPlaneConfigRepository = $dataPlaneConfigRepository;
        $this->dataPlaneId = $dataPlaneId;
    }

    public function encrypt($data, string $componentId, string $projectId, ?BranchType $branchType)
    {
        return $this->getEncryptor()->encrypt($data, $componentId, $projectId, $branchType);
    }

    public function decrypt($data, string $componentId, string $projectId, ?string $configId, ?BranchType $branchType)
    {
        return $this->getEncryptor()->decrypt($data, $componentId, $projectId, $configId, $branchType);
    }

    private function getEncryptor(): JobObjectEncryptor
    {
        if ($this->dataPlaneObjectEncryptor !== null) {
            return $this->dataPlaneObjectEncryptor;
        }

        $dataPlaneConfig = $this->dataPlaneConfigRepository->fetchDataPlaneConfig($this->dataPlaneId);

        return $this->dataPlaneObjectEncryptor = new JobObjectEncryptor(
            $dataPlaneConfig->getEncryption()->createEncryptor(),
        );
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;

class DataPlaneJobObjectEncryptor implements JobObjectEncryptorInterface
{
    private DataPlaneConfig $dataPlaneConfig;

    private ?JobObjectEncryptor $dataPlaneObjectEncryptor;

    public function __construct(DataPlaneConfig $dataPlaneConfig)
    {
        $this->dataPlaneConfig = $dataPlaneConfig;
    }

    public function encrypt($data, string $componentId, string $projectId)
    {
        if ($this->dataPlaneObjectEncryptor === null) {
            $this->dataPlaneObjectEncryptor = new JobObjectEncryptor(
                $this->dataPlaneConfig->getEncryption()->createEncryptor()
            );
        }

        return $this->dataPlaneObjectEncryptor->encrypt($data, $componentId, $projectId);
    }

    public function decrypt($data, string $componentId, string $projectId, ?string $configId)
    {
        if ($this->dataPlaneObjectEncryptor === null) {
            $this->dataPlaneObjectEncryptor = new JobObjectEncryptor(
                $this->dataPlaneConfig->getEncryption()->createEncryptor()
            );
        }

        return $this->dataPlaneObjectEncryptor->decrypt($data, $componentId, $projectId, $configId);
    }
}

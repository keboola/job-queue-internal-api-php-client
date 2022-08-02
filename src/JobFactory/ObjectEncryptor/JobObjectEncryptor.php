<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

use Keboola\ObjectEncryptor\ObjectEncryptor;

class JobObjectEncryptor implements JobObjectEncryptorInterface
{
    private ObjectEncryptor $objectEncryptor;

    public function __construct(ObjectEncryptor $objectEncryptor)
    {
        $this->objectEncryptor = $objectEncryptor;
    }

    public function encrypt($data, string $componentId, string $projectId)
    {
        return $this->objectEncryptor->decryptForProject(
            $data,
            $componentId,
            $projectId,
        );
    }

    public function decrypt($data, string $componentId, string $projectId, ?string $configId)
    {
        if ($configId) {
            return $this->objectEncryptor->decryptForConfiguration(
                $data,
                $componentId,
                $projectId,
                $configId,
            );
        }

        return $this->objectEncryptor->decryptForProject(
            $data,
            $componentId,
            $projectId,
        );
    }
}

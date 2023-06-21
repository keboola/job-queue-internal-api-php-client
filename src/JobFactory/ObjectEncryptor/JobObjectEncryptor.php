<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\PermissionChecker\BranchType;

class JobObjectEncryptor implements JobObjectEncryptorInterface
{
    private ObjectEncryptor $objectEncryptor;

    public function __construct(ObjectEncryptor $objectEncryptor)
    {
        $this->objectEncryptor = $objectEncryptor;
    }

    public function encrypt($data, string $componentId, string $projectId, ?BranchType $branchType)
    {
        if ($branchType !== null) {
            return $this->objectEncryptor->encryptForBranchType(
                $data,
                $componentId,
                $projectId,
                $branchType->value,
            );
        }

        return $this->objectEncryptor->encryptForProject(
            $data,
            $componentId,
            $projectId,
        );
    }

    public function decrypt($data, string $componentId, string $projectId, ?string $configId, ?BranchType $branchType)
    {
        /* When configId is null, the decryptForBranchType has to be used, because configId is required parameter.
            branchType is always known, but for jobs created before branchType was introduced, it is null. This is what
            drives the logic here, not the contents of the cipher! The contents of any cipher can be decrypted with
            decryptForBranchTypeConfiguration which encapsulates all wrappers that might come in use here. See
            https://github.com/keboola/object-encryptor/blob/46555af72554a860fedf651198f520ff6e34bd31/tests/ObjectEncryptorTest.php#L962
        */
        if ($branchType !== null) {
            if ($configId) {
                return $this->objectEncryptor->decryptForBranchTypeConfiguration(
                    $data,
                    $componentId,
                    $projectId,
                    $configId,
                    $branchType->value,
                );
            }

            return $this->objectEncryptor->decryptForBranchType(
                $data,
                $componentId,
                $projectId,
                $branchType->value,
            );
        }
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

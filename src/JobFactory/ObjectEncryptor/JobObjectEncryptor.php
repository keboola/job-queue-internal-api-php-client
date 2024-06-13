<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\PermissionChecker\BranchType;
use stdClass;

class JobObjectEncryptor
{
    private ObjectEncryptor $objectEncryptor;

    public function __construct(ObjectEncryptor $objectEncryptor)
    {
        $this->objectEncryptor = $objectEncryptor;
    }

    /**
     * @template T of string|array|stdClass
     * @param T $data
     * @return T
     */
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

    /**
     * @template T of string|array|stdClass
     * @param T $data
     * @return T
     */
    public function decrypt($data, string $componentId, string $projectId, ?string $configId, BranchType $branchType)
    {
        /* When configId is null, the decryptForBranchType has to be used, because configId is required parameter.
            This is what drives the logic here, not the contents of the cipher! The contents of any cipher can be
            decrypted with decryptForBranchTypeConfiguration which encapsulates all wrappers that might come in use
            here. See
            https://github.com/keboola/object-encryptor/blob/46555af72554a860fedf651198f520ff6e34bd31/tests/ObjectEncryptorTest.php#L962
        */
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
}

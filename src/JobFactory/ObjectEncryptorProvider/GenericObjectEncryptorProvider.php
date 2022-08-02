<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;
use Keboola\ObjectEncryptor\ObjectEncryptor;

class GenericObjectEncryptorProvider implements ObjectEncryptorProviderInterface
{
    private ObjectEncryptor $objectEncryptor;

    public function __construct(ObjectEncryptor $objectEncryptor)
    {
        $this->objectEncryptor = $objectEncryptor;
    }

    public function getJobEncryptor(array $jobData): JobObjectEncryptorInterface
    {
        return new JobObjectEncryptor($this->objectEncryptor);
    }
}

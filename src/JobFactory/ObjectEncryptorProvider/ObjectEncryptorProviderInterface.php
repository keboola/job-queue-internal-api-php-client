<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;

interface ObjectEncryptorProviderInterface
{
    public function getJobEncryptor(array $jobData): JobObjectEncryptorInterface;
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class ExistingJobFactory extends JobFactory
{
    public function __construct(
        private readonly StorageClientPlainFactory $storageClientFactory,
        private readonly JobObjectEncryptor $objectEncryptor,
    ) {
    }

    public function loadFromExistingJobData(array $data): JobInterface
    {
        $data = $this->validateJobData($data, FullJobDefinition::class);

        return new Job($this->objectEncryptor, $this->storageClientFactory, $data);
    }
}

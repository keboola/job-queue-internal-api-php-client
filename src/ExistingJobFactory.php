<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\JobFactory\ElasticJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobObjectEncryptor;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

/**
 * @implements ExistingJobFactoryInterface<JobInterface>
 */
class ExistingJobFactory extends JobFactory implements ExistingJobFactoryInterface
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

    public function loadFromElasticJobData(array $data): JobInterface
    {
        $data = $this->validateJobData($data, ElasticJobDefinition::class);

        return new Job($this->objectEncryptor, $this->storageClientFactory, $data);
    }
}

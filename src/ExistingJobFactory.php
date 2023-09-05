<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\ObjectEncryptorProviderInterface;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class ExistingJobFactory extends JobFactory
{
    private StorageClientPlainFactory $storageClientFactory;
    private ObjectEncryptorProviderInterface $objectEncryptorProvider;

    public function __construct(
        StorageClientPlainFactory $storageClientFactory,
        ObjectEncryptorProviderInterface $objectEncryptorProvider,
    ) {
        $this->storageClientFactory = $storageClientFactory;
        $this->objectEncryptorProvider = $objectEncryptorProvider;
    }

    public function loadFromExistingJobData(array $data): JobInterface
    {
        $data = $this->validateJobData($data, FullJobDefinition::class);

        $encryptor = $this->objectEncryptorProvider->getJobEncryptor($data);
        return new Job($encryptor, $this->storageClientFactory, $data);
    }
}

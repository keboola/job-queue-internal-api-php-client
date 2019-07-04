<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\StorageClientFactory;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\ObjectEncryptor\Wrapper\ProjectWrapper;
use Keboola\StorageApi\ClientException as StorageClientException;

class JobFactory
{
    /** @var StorageClientFactory */
    private $storageClientFactory;

    /** @var ObjectEncryptorFactory */
    private $objectEncryptorFactory;

    public function __construct(
        StorageClientFactory $storageClientFactory,
        ObjectEncryptorFactory $objectEncryptorFactory
    ) {
        $this->storageClientFactory = $storageClientFactory;
        $this->objectEncryptorFactory = $objectEncryptorFactory;
    }

    public function createNewJob(array $data): Job
    {
        $data = $this->validateNewJobData($data);
        $data = $this->initializeNewJobData($data);
        $data = $this->validateJobData($data);
        $job = new Job($data);
        return $job;
    }

    public function loadExistingJob(array $data): Job
    {
        $data = $this->validateJobData($data);
        return new Job($data);
    }

    private function validateNewJobData(array $data): array
    {
        if (empty($data['params']['component'])) {
            throw new ClientException('Invalid job data: missing params.component');
        }
        if (empty($data['params']['mode'])) {
            throw new ClientException('Invalid job data: missing params.mode');
        }
        if (empty($data['token']['token'])) {
            throw new ClientException('Invalid job data: missing token.token');
        }
        if (isset($data['params']['row']) && !is_scalar($data['params']['row'])) {
            throw new ClientException(
                sprintf(
                    'Unsupported row value "%s". Scalar row ID is required.',
                    var_export($data['params']['row'], true)
                )
            );
        }
        if (isset($data['params']['configData']) && !is_array($data['params']['configData'])) {
            throw new ClientException(
                sprintf(
                    'Unsupported configData value "%s". Array is required.',
                    var_export($data['params']['configData'], true)
                )
            );
        }
        return $data;
    }

    private function validateJobData(array $data): array
    {
        $data = $this->validateNewJobData($data);
        if (empty($data['id'])) {
            throw new ClientException('Invalid job data: missing id');
        }
        if (empty($data['project']['id'])) {
            throw new ClientException('Invalid job data: missing project.id');
        }
        if (empty($data['token']['id'])) {
            throw new ClientException('Invalid job data: missing token.id');
        }
        // todo check valid values for status
        if (empty($data['status'])) {
            throw new ClientException('Invalid job data: missing status');
        }
        return $data;
    }

    private function initializeNewJobData(array $data): array
    {
        try {
            $client = $this->storageClientFactory->getClient($data['token']['token']);
            $tokenInfo = $client->verifyToken();
            $data['project']['id'] = $tokenInfo['owner']['id'];
            $data['token']['id'] = $tokenInfo['id'];
            $data['status'] = Job::STATUS_CREATED;
            $data['id'] = $client->generateId();
        } catch (StorageClientException $e) {
            throw new ClientException('Cannot create job: ' . $e->getMessage(), $e->getCode(), $e);
        }
        $this->objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $this->objectEncryptorFactory->setComponentId($data ['params']['component']);
        $this->objectEncryptorFactory->setStackId(
            parse_url($this->storageClientFactory->getStorageApiUrl(), PHP_URL_HOST)
        );
        $data['token']['token'] = $this->objectEncryptorFactory->getEncryptor()->encrypt(
            $data['token']['token'],
            ProjectWrapper::class
        );
        return $data;
    }
}

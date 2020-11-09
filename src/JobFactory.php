<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\StorageClientFactory;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\ObjectEncryptor\Wrapper\ProjectWrapper;
use Keboola\StorageApi\ClientException as StorageClientException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class JobFactory
{
    public const STATUS_CANCELLED = 'cancelled';
    public const STATUS_CREATED = 'created';
    public const STATUS_ERROR = 'error';
    public const STATUS_PROCESSING = 'processing';
    public const STATUS_SUCCESS = 'success';
    public const STATUS_TERMINATED = 'terminated';
    public const STATUS_TERMINATING = 'terminating';
    public const STATUS_WAITING = 'waiting';
    public const STATUS_WARNING = 'warning';

    public const DESIRED_STATUS_PROCESSING = 'processing';
    public const DESIRED_STATUS_TERMINATING = 'terminating';

    /** @var StorageClientFactory */
    private $storageClientFactory;

    /** @var ObjectEncryptorFactory */
    private $objectEncryptorFactory;

    public function __construct(
        StorageClientFactory $storageClientFactory,
        ObjectEncryptorFactory $objectEncryptorFactory
    ) {
        $this->storageClientFactory = $storageClientFactory;
        // it's important to clone here because we change state of the factory!,
        // this is tested by JobFactoryTest::testEncryptionFactoryIsolation()
        $this->objectEncryptorFactory = clone $objectEncryptorFactory;
    }

    public static function getFinishedStatuses(): array
    {
        return [self::STATUS_SUCCESS, self::STATUS_WARNING, self::STATUS_ERROR, self::STATUS_CANCELLED,
            self::STATUS_TERMINATED];
    }

    public static function getAllStatuses(): array
    {
        return [self::STATUS_CANCELLED, self::STATUS_CREATED, self::STATUS_ERROR, self::STATUS_PROCESSING,
            self::STATUS_SUCCESS, self::STATUS_TERMINATED, self::STATUS_TERMINATING, self::STATUS_WAITING,
            self::STATUS_WARNING];
    }

    public static function getKillableStatuses(): array
    {
        return [self::STATUS_CREATED, self::STATUS_WAITING, self::STATUS_PROCESSING];
    }

    public static function getLegacyComponents(): array
    {
        return ['orchestrator', 'transformation', 'provisioning'];
    }

    public function createNewJob(array $data): Job
    {
        $data = $this->validateJobData($data, NewJobDefinition::class);
        $data = $this->initializeNewJobData($data);
        $data = $this->validateJobData($data, FullJobDefinition::class);
        return new Job($this->objectEncryptorFactory, $data);
    }

    public function loadFromExistingJobData(array $data): Job
    {
        $data = $this->validateJobData($data, FullJobDefinition::class);
        return new Job($this->objectEncryptorFactory, $data);
    }

    public function modifyJob(Job $job, array $patchData): Job
    {
        $data = $job->jsonSerialize();
        $data = array_replace_recursive($data, $patchData);
        $data = $this->validateJobData($data, FullJobDefinition::class);
        return new Job($this->objectEncryptorFactory, $data);
    }

    private function validateJobData(array $data, string $validatorClass): array
    {
        try {
            /** @var NewJobDefinition $jobDefinition */
            $jobDefinition = new $validatorClass();
            $data = $jobDefinition->processData($data);
            return $data;
        } catch (InvalidConfigurationException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    private function initializeNewJobData(array $data): array
    {
        try {
            $client = $this->storageClientFactory->getClient($data['token']);
            $tokenInfo = $client->verifyToken();
            $jobId = $client->generateId();
            $runId = empty($data['parentRunId']) ? $jobId : $data['parentRunId'] . Job::RUN_ID_DELIMITER . $jobId;
        } catch (StorageClientException $e) {
            throw new ClientException(
                'Cannot create job: "' . $e->getMessage() . '".',
                $e->getCode(),
                $e
            );
        }
        $this->objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $this->objectEncryptorFactory->setComponentId($data['component']);
        $this->objectEncryptorFactory->setStackId(
            (string) parse_url($this->storageClientFactory->getStorageApiUrl(), PHP_URL_HOST)
        );
        $encryptedToken = $this->objectEncryptorFactory->getEncryptor()->encrypt(
            $data['token'],
            ProjectWrapper::class
        );

        return [
            'id' => $jobId,
            'runId' => $runId,
            'projectId' => $tokenInfo['owner']['id'],
            'projectName' => $tokenInfo['owner']['name'],
            'tokenId' => $tokenInfo['id'],
            'tokenString' => $encryptedToken,
            'tokenDescription' => $tokenInfo['description'],
            'status' => self::STATUS_CREATED,
            'desiredStatus' => self::DESIRED_STATUS_PROCESSING,
            'mode' => $data['mode'],
            'component' => $data['component'],
            'configId' => $data['config'] ?? null,
            'configData' => $data['configData'] ?? null, //@todo encrypt configData?
            'configRowId' => $data['row'] ?? null,
            'tag' => $data['tag'] ?? null,
            'result' => [],
            'usageData' => [],
            'isFinished' => false
        ];
    }
}

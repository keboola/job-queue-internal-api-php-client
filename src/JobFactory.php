<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigRepository;
use Keboola\JobQueueInternalClient\DataPlane\DataPlaneObjectEncryptorFactory;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\Behavior;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
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

    public const TYPE_STANDARD = 'standard';
    public const TYPE_ROW_CONTAINER = 'container';
    public const TYPE_PHASE_CONTAINER = 'phaseContainer';
    public const TYPE_ORCHESTRATION_CONTAINER = 'orchestrationContainer';

    public const PARALLELISM_INFINITY = 'infinity';
    public const ORCHESTRATOR_COMPONENT = 'keboola.orchestrator';

    public const PAY_AS_YOU_GO_FEATURE = 'pay-as-you-go';

    private StorageClientPlainFactory $storageClientFactory;
    private ObjectEncryptor $controlPlaneObjectEncryptor;
    private DataPlaneObjectEncryptorFactory $objectEncryptorFactory;
    private DataPlaneConfigRepository $dataPlaneConfigRepository;
    private bool $supportsDataPlanes;

    public function __construct(
        StorageClientPlainFactory $storageClientFactory,
        ObjectEncryptor $controlPlaneEncryptor,
        DataPlaneObjectEncryptorFactory $objectEncryptorFactory,
        DataPlaneConfigRepository $dataPlaneConfigRepository,
        bool $supportsDataPlanes
    ) {
        $this->storageClientFactory = $storageClientFactory;
        $this->controlPlaneObjectEncryptor = $controlPlaneEncryptor;
        $this->objectEncryptorFactory = $objectEncryptorFactory;
        $this->dataPlaneConfigRepository = $dataPlaneConfigRepository;
        $this->supportsDataPlanes = $supportsDataPlanes;
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

    public static function getAllDesiredStatuses(): array
    {
        return [self::DESIRED_STATUS_PROCESSING, self::DESIRED_STATUS_TERMINATING];
    }

    public static function getKillableStatuses(): array
    {
        return [self::STATUS_CREATED, self::STATUS_WAITING, self::STATUS_PROCESSING];
    }

    public static function getAllowedJobTypes(): array
    {
        return [self::TYPE_STANDARD, self::TYPE_ROW_CONTAINER,
            self::TYPE_PHASE_CONTAINER, self::TYPE_ORCHESTRATION_CONTAINER,
        ];
    }

    public static function getAllowedParallelismValues(): array
    {
        $intValues = array_map(
            fn ($item) => (string) $item,
            range(0, 100)
        );
        return array_merge($intValues, ['infinity', null]);
    }

    public static function getLegacyComponents(): array
    {
        return ['orchestrator', 'transformation', 'provisioning'];
    }

    public function createNewJob(array $data): JobInterface
    {
        $data = $this->validateJobData($data, NewJobDefinition::class);

        try {
            $client = $this->storageClientFactory->createClientWrapper(new ClientOptions(
                null,
                $data['#tokenString']
            ))->getBasicClient();
            $tokenInfo = $client->verifyToken();
            $jobId = $client->generateId();
            $runId = empty($data['parentRunId']) ?
                $jobId :
                $data['parentRunId'] . JobInterface::RUN_ID_DELIMITER . $jobId;
        } catch (StorageClientException $e) {
            throw new ClientException(
                'Cannot create job: "' . $e->getMessage() . '".',
                $e->getCode(),
                $e
            );
        }

        if (!empty($data['variableValuesId']) && !empty($data['variableValuesData']['values'])) {
            throw new ClientException(
                'Provide either "variableValuesId" or "variableValuesData", but not both.'
            );
        }

        if ($this->supportsDataPlanes) {
            $dataPlaneConfig = $this->dataPlaneConfigRepository->fetchProjectDataPlane(
                (string) $tokenInfo['owner']['id'],
            );
        } else {
            $dataPlaneConfig = null;
        }

        $jobData = [
            'id' => $jobId,
            'runId' => $runId,
            'projectId' => $tokenInfo['owner']['id'],
            'projectName' => $tokenInfo['owner']['name'],
            'dataPlaneId' => $dataPlaneConfig['id'] ?? null,
            'tokenId' => $tokenInfo['id'],
            '#tokenString' => $data['#tokenString'],
            'tokenDescription' => $tokenInfo['description'],
            'status' => self::STATUS_CREATED,
            'desiredStatus' => self::DESIRED_STATUS_PROCESSING,
            'mode' => $data['mode'],
            'componentId' => $data['componentId'],
            'configId' => $data['configId'] ?? null,
            'configData' => $data['configData'] ?? null,
            'configRowIds' => $data['configRowIds'] ?? null,
            'tag' => $data['tag'] ?? null,
            'parallelism' => $data['parallelism'] ?? null,
            'backend' => $data['backend'] ?? null,
            'behavior' => $data['behavior'] ?? (new Behavior())->toDataArray(),
            'result' => [],
            'usageData' => [],
            'isFinished' => false,
            'branchId' => $data['branchId'] ?? null,
            'variableValuesId' => $data['variableValuesId'] ?? null,
            'variableValuesData' => $data['variableValuesData'] ?? [],
            'orchestrationJobId' => $data['orchestrationJobId'] ?? null,
        ];
        $resolver = new JobRuntimeResolver($this->storageClientFactory);
        $jobData = $resolver->resolveJobData($jobData, $tokenInfo);
        // set type after resolving parallelism
        $jobData['type'] = $data['type'] ?? $this->getJobType($jobData);

        if ($dataPlaneConfig !== null) {
            $objectEncryptor = $this->objectEncryptorFactory->getObjectEncryptor(
                $dataPlaneConfig['id'],
                $dataPlaneConfig['parameters'],
            );
        } else {
            $objectEncryptor = $this->controlPlaneObjectEncryptor;
        }

        $data = $objectEncryptor->encryptForProject(
            $jobData,
            (string) $data['componentId'],
            (string) $tokenInfo['owner']['id']
        );

        $data = $this->validateJobData($data, FullJobDefinition::class);
        return new Job($objectEncryptor, $this->storageClientFactory, $data);
    }

    public function loadFromExistingJobData(array $data): JobInterface
    {
        $data = $this->validateJobData($data, FullJobDefinition::class);

        if ($data['dataPlaneId'] ?? null) {
            $dataPlaneConfig = $this->dataPlaneConfigRepository->fetchDataPlaneConfig($data['dataPlaneId']);
        } else {
            $dataPlaneConfig = null;
        }

        if ($dataPlaneConfig !== null) {
            $objectEncryptor = $this->objectEncryptorFactory->getObjectEncryptor(
                $data['dataPlaneId'],
                $dataPlaneConfig,
            );
        } else {
            $objectEncryptor = $this->controlPlaneObjectEncryptor;
        }

        return new Job($objectEncryptor, $this->storageClientFactory, $data);
    }

    public function modifyJob(JobInterface $job, array $patchData): JobInterface
    {
        $data = $job->jsonSerialize();
        $data = array_replace_recursive($data, $patchData);

        return $this->loadFromExistingJobData($data);
    }

    /**
     * @param class-string<FullJobDefinition|NewJobDefinition> $validatorClass
     */
    private function validateJobData(array $data, string $validatorClass): array
    {
        try {
            return (new $validatorClass())->processData($data);
        } catch (InvalidConfigurationException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    private function getJobType(array $data): string
    {
        if ((intval($data['parallelism']) > 0) || $data['parallelism'] === self::PARALLELISM_INFINITY) {
            return self::TYPE_ROW_CONTAINER;
        }

        if ($data['componentId'] !== self::ORCHESTRATOR_COMPONENT) {
            return self::TYPE_STANDARD;
        }

        if (isset($data['configData']['phaseId']) && (string) ($data['configData']['phaseId']) !== '') {
            return self::TYPE_PHASE_CONTAINER;
        }

        return self::TYPE_ORCHESTRATION_CONTAINER;
    }
}

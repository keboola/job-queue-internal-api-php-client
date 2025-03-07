<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\Behavior;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class NewJobFactory extends JobFactory
{
    public function __construct(
        private readonly StorageClientPlainFactory $storageClientFactory,
        private readonly JobRuntimeResolver $jobRuntimeResolver,
        private readonly JobObjectEncryptor $objectEncryptor,
    ) {
    }

    public function createNewJob(array $data): JobInterface
    {
        $data = $this->validateJobData($data, NewJobDefinition::class);

        try {
            $client = $this->storageClientFactory->createClientWrapper(new ClientOptions(
                null,
                $data['#tokenString'],
            ))->getBasicClient();
            $tokenInfo = $client->verifyToken();
            $jobId = $client->generateId();
            $runId = empty($data['parentRunId']) ? $jobId :
                $data['parentRunId'] . JobInterface::RUN_ID_DELIMITER . $jobId;
        } catch (StorageClientException $e) {
            throw new ClientException(
                'Cannot create job: "' . $e->getMessage() . '".',
                $e->getCode(),
                $e,
            );
        }

        if (!empty($data['variableValuesId']) && !empty($data['variableValuesData']['values'])) {
            throw new ClientException(
                'Provide either "variableValuesId" or "variableValuesData", but not both.',
            );
        }

        $projectId = (string) $tokenInfo['owner']['id'];

        $jobData = [
            'id' => $jobId,
            'deduplicationId' => $data['deduplicationId'] ?? null,
            'runId' => $runId,
            'projectId' => $projectId,
            'projectName' => $tokenInfo['owner']['name'],
            'tokenId' => $tokenInfo['id'],
            '#tokenString' => $data['#tokenString'],
            'tokenDescription' => $tokenInfo['description'],
            'status' => JobInterface::STATUS_CREATED,
            'desiredStatus' => JobInterface::DESIRED_STATUS_PROCESSING,
            'mode' => $data['mode'],
            'componentId' => $data['componentId'],
            'configId' => $data['configId'] ?? null,
            'configData' => $data['configData'] ?? null,
            'configRowIds' => $data['configRowIds'] ?? null,
            'tag' => $data['tag'] ?? null,
            'parallelism' => $data['parallelism'] ?? null,
            'backend' => $data['backend'] ?? null,
            'executor' => $data['executor'] ?? null,
            'behavior' => $data['behavior'] ?? (new Behavior())->toDataArray(),
            'result' => [],
            'usageData' => [],
            'isFinished' => false,
            'branchId' => $data['branchId'] ?? null,
            'type' => $data['type'] ?? null,
            'variableValuesId' => $data['variableValuesId'] ?? null,
            'variableValuesData' => $data['variableValuesData'] ?? [],
            'orchestrationJobId' => $data['orchestrationJobId'] ?? null,
            'orchestrationTaskId' => $data['orchestrationTaskId'] ?? null,
            'orchestrationPhaseId' => $data['orchestrationPhaseId'] ?? null,
            'onlyOrchestrationTaskIds' => $data['onlyOrchestrationTaskIds'] ?? null,
            'previousJobId' => $data['previousJobId'] ?? null,
        ];
        $jobData = $this->jobRuntimeResolver->resolveJobData($jobData, $tokenInfo);

        $data = $this->objectEncryptor->encrypt(
            $jobData,
            (string) $data['componentId'],
            (string) $tokenInfo['owner']['id'],
            BranchType::from($jobData['branchType']),
            $tokenInfo['owner']['features'],
        );

        $data = $this->validateJobData($data, FullJobDefinition::class);
        return new Job($this->objectEncryptor, $this->storageClientFactory, $data);
    }
}

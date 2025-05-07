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
            $clientWrapper = $this->storageClientFactory->createClientWrapper(new ClientOptions(
                token: $data['#tokenString'],
            ));
            $token = $clientWrapper->getToken();

            $jobId = $clientWrapper->getBasicClient()->generateId();
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

        $jobData = [
            'id' => $jobId,
            'deduplicationId' => $data['deduplicationId'] ?? null,
            'runId' => $runId,
            'projectId' => $token->getProjectId(),
            'projectName' => $token->getProjectName(),
            'tokenId' => $token->getTokenId(),
            '#tokenString' => $data['#tokenString'],
            'tokenDescription' => $token->getTokenDesc(),
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
            'delay' => $data['delay'] ?? null,
            'delayStartTime' => $data['delayStartTime'] ?? null,
        ];
        $jobData = $this->jobRuntimeResolver->resolveJobData($jobData, $token);

        $data = $this->objectEncryptor->encrypt(
            $jobData,
            (string) $data['componentId'],
            $token->getProjectId(),
            BranchType::from($jobData['branchType']),
            $token->getFeatures(),
        );

        $data = $this->validateJobData($data, FullJobDefinition::class);
        return new Job($this->objectEncryptor, $this->storageClientFactory, $data);
    }
}

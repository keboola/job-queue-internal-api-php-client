<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\Behavior;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobRuntimeResolver;
use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\DataPlaneObjectEncryptorProvider;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Psr\Log\LoggerInterface;

class NewJobFactory extends JobFactory
{
    public function __construct(
        private StorageClientPlainFactory $storageClientFactory,
        private JobRuntimeResolver $jobRuntimeResolver,
        private DataPlaneObjectEncryptorProvider $objectEncryptorProvider,
        private LoggerInterface $logger,
    ) {
    }

    public function createNewJob(array $data): JobInterface
    {
        $data = $this->validateJobData($data, NewJobDefinition::class);

        $branchId = $data['branchId'] ?? null;
        $storageClientWrapper = $this->storageClientFactory->createClientWrapper(new ClientOptions(
            token: $data['#tokenString'],
            branchId: $branchId ? ((string) $branchId) : null,
        ));

        try {
            if ($branchId === null) {
                $this->logger->warning('Not setting branchId is deprecated, set actual branch ID');
                $branchId = $storageClientWrapper->getBranchId();
                $branchType = BranchType::DEFAULT;
            } elseif ($branchId === 'default') {
                $this->logger->warning('Using branchId alias "default" is deprecated, set actual branch ID');
                $branchId = $storageClientWrapper->getBranchId();
                $branchType = BranchType::DEFAULT;
            } else {
                $branchType = $storageClientWrapper->getDefaultBranch()['branchId'] === (string) $branchId
                    ? BranchType::DEFAULT
                    : BranchType::DEV
                ;
            }

            $client = $storageClientWrapper->getBasicClient();
            $tokenInfo = $client->verifyToken();
            $jobId = $client->generateId();
            $runId = empty($data['parentRunId']) ? $jobId :
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

        $projectId = (string) $tokenInfo['owner']['id'];
        $dataPlaneConfig = $this->objectEncryptorProvider->resolveProjectDataPlaneConfig($projectId);
        $encryptor = $this->objectEncryptorProvider->getProjectObjectEncryptor($dataPlaneConfig);

        $jobData = [
            'id' => $jobId,
            'runId' => $runId,
            'projectId' => $projectId,
            'projectName' => $tokenInfo['owner']['name'],
            'dataPlaneId' => $dataPlaneConfig ? $dataPlaneConfig->getId() : null,
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
            'branchId' => (string) $branchId,
            'branchType' => $branchType->value,
            'type' => $data['type'] ?? null,
            'variableValuesId' => $data['variableValuesId'] ?? null,
            'variableValuesData' => $data['variableValuesData'] ?? [],
            'orchestrationJobId' => $data['orchestrationJobId'] ?? null,
        ];
        $jobData = $this->jobRuntimeResolver->resolveJobData($jobData, $tokenInfo);

        if (in_array(self::PROTECTED_DEFAULT_BRANCH_FEATURE, $tokenInfo['owner']['features'])) {
            $data = $encryptor->encrypt(
                $jobData,
                (string) $data['componentId'],
                (string) $tokenInfo['owner']['id'],
                BranchType::from($jobData['branchType']),
            );
        } else {
            $data = $encryptor->encrypt(
                $jobData,
                (string) $data['componentId'],
                (string) $tokenInfo['owner']['id'],
                null,
            );
        }

        $data = $this->validateJobData($data, FullJobDefinition::class);
        return new Job($encryptor, $this->storageClientFactory, $data);
    }
}

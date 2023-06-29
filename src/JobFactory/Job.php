<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use DateTimeImmutable;
use DateTimeZone;
use JsonSerializable;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Backend;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Executor;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\StorageApi\Components as ComponentsApiClient;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Symfony\Component\Uid\Uuid;
use Throwable;

class Job implements JsonSerializable, JobInterface
{
    private JobObjectEncryptorInterface $objectEncryptor;
    private StorageClientPlainFactory $storageClientFactory;
    private array $data;

    private ?DateTimeImmutable $endTime;
    private ?DateTimeImmutable $startTime;
    private ?string $tokenDecrypted = null;
    private ?string $executionTokenDecrypted = null;
    private ?array $componentConfigurationDecrypted = null;
    private ?array $configDataDecrypted = null;

    private ?ComponentsApiClient $componentsApiClient = null;
    private ?ComponentSpecification $componentSpecification = null;
    private ?array $componentConfiguration = null;
    private ?array $projectFeatures = null;

    public function __construct(
        JobObjectEncryptorInterface $objectEncryptor,
        StorageClientPlainFactory $storageClientFactory,
        array $data
    ) {
        $this->objectEncryptor = $objectEncryptor;
        $this->storageClientFactory = $storageClientFactory;
        $this->data = $data;

        $this->data['isFinished'] = in_array($this->getStatus(), JobInterface::STATUSES_FINISHED);
        $this->data['parentRunId'] = $this->getParentRunId();
        $this->startTime = null;
        $this->endTime = null;
        try {
            if (!empty($this->data['startTime'])) {
                $this->startTime = new DateTimeImmutable($this->data['startTime'], new DateTimeZone('utc'));
            }
        } catch (Throwable $e) {
            // intentionally empty
        }
        try {
            if (!empty($this->data['endTime'])) {
                $this->endTime = new DateTimeImmutable($this->data['endTime'], new DateTimeZone('utc'));
            }
        } catch (Throwable $e) {
            // intentionally empty
        }
    }

    public function getId(): string
    {
        return $this->data['id'];
    }

    public function getComponentId(): string
    {
        return $this->data['componentId'] ?? '';
    }

    public function getConfigData(): array
    {
        return $this->data['configData'] ?? [];
    }

    public function getConfigId(): ?string
    {
        return $this->data['configId'] ?? null;
    }

    public function getMode(): string
    {
        return $this->data['mode'];
    }

    public function getProjectId(): string
    {
        return $this->data['projectId'];
    }

    public function getProjectName(): string
    {
        return $this->data['projectName'];
    }

    public function getDataPlaneId(): ?string
    {
        return $this->data['dataPlaneId'] ?? null;
    }

    public function getResult(): array
    {
        return $this->data['result'] ?? [];
    }

    public function getConfigRowIds(): array
    {
        return $this->data['configRowIds'] ?? [];
    }

    public function getStatus(): string
    {
        return $this->data['status'];
    }

    public function getDesiredStatus(): string
    {
        return $this->data['desiredStatus'];
    }

    public function getTag(): ?string
    {
        return $this->data['tag'] ?? null;
    }

    public function getTokenString(): string
    {
        return $this->data['#tokenString'];
    }

    public function getTokenId(): string
    {
        return $this->data['tokenId'];
    }

    public function getTokenDescription(): string
    {
        return $this->data['tokenDescription'];
    }

    public function getParentRunId(): string
    {
        $parts = explode(self::RUN_ID_DELIMITER, $this->getRunId());
        array_pop($parts);
        return implode(self::RUN_ID_DELIMITER, $parts);
    }

    public function getRunId(): string
    {
        return $this->data['runId'];
    }

    public function isFinished(): bool
    {
        return (bool) $this->data['isFinished'];
    }

    public function getUsageData(): array
    {
        return $this->data['usageData'] ?? [];
    }

    public function getBackend(): Backend
    {
        return Backend::fromDataArray($this->data['backend'] ?? []);
    }

    public function getExecutor(): Executor
    {
        return isset($this->data['executor']) ? Executor::from($this->data['executor']) : Executor::getDefault();
    }

    public function getType(): string
    {
        return $this->data['type'] ?? JobInterface::TYPE_STANDARD;
    }

    public function getParallelism(): ?string
    {
        return $this->data['parallelism'] ?? null;
    }

    public function getBehavior(): Behavior
    {
        return isset($this->data['behavior'])
            ? Behavior::fromDataArray($this->data['behavior'])
            : new Behavior();
    }

    public function jsonSerialize(): array
    {
        return $this->data;
    }
    public function getTokenDecrypted(): string
    {
        return $this->tokenDecrypted ??= $this->objectEncryptor->decrypt(
            $this->getTokenString(),
            $this->getComponentId(),
            $this->getProjectId(),
            $this->getConfigId(),
            $this->getBranchType(),
        );
    }

    public function getExecutionTokenDecrypted(string $applicationToken): ?string
    {
        if (in_array(JobFactory::PROTECTED_DEFAULT_BRANCH_FEATURE, $this->getProjectFeatures())) {
            $this->executionTokenDecrypted ??= $this->createPrivilegedToken($applicationToken);
        }

        return $this->executionTokenDecrypted = null;
    }

    public function getComponentConfigurationDecrypted(): ?array
    {
        if ($this->getConfigId() === null) {
            return null;
        }

        return $this->componentConfigurationDecrypted ??= $this->objectEncryptor->decrypt(
            $this->getComponentConfiguration(),
            $this->getComponentId(),
            $this->getProjectId(),
            $this->getConfigId(),
            $this->getBranchType(),
        );
    }

    public function getConfigDataDecrypted(): array
    {
        return $this->configDataDecrypted ??= $this->objectEncryptor->decrypt(
            $this->getConfigData(),
            $this->getComponentId(),
            $this->getProjectId(),
            $this->getConfigId(),
            $this->getBranchType(),
        );
    }

    public function getBranchType(): ?BranchType
    {
        return $this->data['branchType'] ? BranchType::from($this->data['branchType']) : null;
    }

    public function getBranchId(): ?string
    {
        return $this->data['branchId'] ?? null;
    }

    public function getVariableValuesId(): ?string
    {
        return $this->data['variableValuesId'] ?? null;
    }

    public function getVariableValuesData(): array
    {
        return $this->data['variableValuesData'] ?? [];
    }

    public function getVariableValues(): VariableValues
    {
        return VariableValues::fromDataArray($this->data);
    }

    public function hasVariables(): bool
    {
        return $this->getVariableValuesId() || !$this->getVariableValues()->isEmpty();
    }

    public function getStartTime(): ?DateTimeImmutable
    {
        return $this->startTime;
    }

    public function getEndTime(): ?DateTimeImmutable
    {
        return $this->endTime;
    }

    public function getDurationSeconds(): ?int
    {
        return isset($this->data['durationSeconds']) ? (int) $this->data['durationSeconds'] : null;
    }

    public function getMetrics(): JobMetrics
    {
        return JobMetrics::fromDataArray($this->data);
    }

    public function isInRunMode(): bool
    {
        return in_array($this->getMode(), [JobInterface::MODE_RUN, JobInterface::MODE_FORCE_RUN]);
    }

    public function getOrchestrationJobId(): ?string
    {
        return $this->data['orchestrationJobId'] ?? null;
    }

    public function getRunnerId(): ?string
    {
        return $this->data['runnerId'] ?? null;
    }

    public function getComponentSpecification(): ComponentSpecification
    {
        if ($this->componentSpecification !== null) {
            return $this->componentSpecification;
        }

        try {
            $data = $this->getComponentsApiClient()->getComponent($this->getComponentId());
        } catch (StorageApiClientException $e) {
            throw new ClientException('Failed to fetch component specification: '.$e->getMessage(), 0, $e);
        }

        return $this->componentSpecification = new ComponentSpecification($data);
    }

    public function getComponentConfiguration(): array
    {
        if ($this->componentConfiguration !== null) {
            return $this->componentConfiguration;
        }

        if (!$this->getConfigId()) {
            throw new ClientException('Can\'t fetch component configuration: job has no configId set');
        }

        try {
            return $this->componentConfiguration = $this->getComponentsApiClient()->getConfiguration(
                $this->getComponentId(),
                $this->getConfigId()
            );
        } catch (StorageApiClientException $e) {
            throw new ClientException('Failed to fetch component configuration: '.$e->getMessage(), 0, $e);
        }
    }

    private function getComponentsApiClient(): ComponentsApiClient
    {
        if ($this->componentsApiClient !== null) {
            return $this->componentsApiClient;
        }

        return $this->componentsApiClient = new ComponentsApiClient(
            $this->getStorageClientWrapper()->getBranchClientIfAvailable()
        );
    }

    public function getProjectFeatures(): array
    {
        if ($this->projectFeatures !== null) {
            return $this->projectFeatures;
        }

        return $this->projectFeatures = $this->getStorageClientWrapper()
            ->getBranchClientIfAvailable()
            ->verifyToken()['owner']['features'];
    }

    public function createPrivilegedToken(string $applicationToken): string
    {
        $tokens = new Tokens($this->getStorageClientWrapper()->getBranchClientIfAvailable());
        $options = new TokenCreateOptions();
        $options->setExpiresIn(3600*24*7);
        $token = $tokens->createTokenPrivilegedInProtectedDefaultBranch($options, $applicationToken);

        return $token['token'];
    }

    public static function generateRunnerId(): string
    {
        return (string) Uuid::v4();
    }

    private function getStorageClientWrapper(): ClientWrapper
    {
        return $this->storageClientFactory->createClientWrapper(
            new ClientOptions(null, $this->getTokenDecrypted(), $this->getBranchId())
        );
    }
}

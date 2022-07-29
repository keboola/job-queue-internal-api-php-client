<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use DateTimeImmutable;
use DateTimeZone;
use JsonSerializable;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use stdClass;
use Throwable;

class Job implements JsonSerializable, JobInterface
{
    public const MODE_RUN = 'run';
    public const MODE_DEBUG = 'debug';
    public const MODE_FORCE_RUN = 'forceRun';

    private ObjectEncryptor $objectEncryptor;
    private StorageClientPlainFactory $storageClientFactory;
    private array $data;

    private ?DateTimeImmutable $endTime;
    private ?DateTimeImmutable $startTime;
    private ?string $tokenDecrypted = null;
    private ?array $configDataDecrypted = null;
    private ?ComponentSpecification $componentSpecification;

    public function __construct(
        ObjectEncryptor $objectEncryptor,
        StorageClientPlainFactory $storageClientFactory,
        array $data
    ) {
        $this->objectEncryptor = $objectEncryptor;
        $this->storageClientFactory = $storageClientFactory;
        $this->data = $data;

        $this->data['isFinished'] = in_array($this->getStatus(), JobFactory::getFinishedStatuses());
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

    public function getType(): string
    {
        return $this->data['type'] ?? JobFactory::TYPE_STANDARD;
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
        if ($this->tokenDecrypted !== null) {
            return $this->tokenDecrypted;
        }

        return $this->tokenDecrypted = $this->decryptData($this->getTokenString());
    }

    public function getConfigDataDecrypted(): array
    {
        if ($this->configDataDecrypted !== null) {
            return $this->configDataDecrypted;
        }

        return $this->configDataDecrypted = $this->decryptData($this->getConfigData());
    }

    /**
     * @template T of array|stdClass|string
     * @param T $data
     * @return T
     */
    private function decryptData($data)
    {
        $componentId = $this->getComponentId();
        $projectId = $this->getProjectId();
        $configId = $this->getConfigId();

        if ($configId) {
            return $this->objectEncryptor->decryptForConfiguration(
                $data,
                $componentId,
                $projectId,
                $configId,
            );
        }

        return $this->objectEncryptor->decryptForProject(
            $data,
            $componentId,
            $projectId,
        );
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
        return in_array($this->getMode(), [self::MODE_RUN, self::MODE_FORCE_RUN]);
    }

    public function getComponentSpecification(): ComponentSpecification
    {
        if (empty($this->componentSpecification)) {
            $client = $this->storageClientFactory->createClientWrapper(
                new ClientOptions(null, $this->getTokenDecrypted(), $this->getBranchId())
            );
            $componentsApi = new Components($client->getBranchClientIfAvailable());
            $data = $componentsApi->getComponent($this->getComponentId());
            $this->componentSpecification = new ComponentSpecification($data);
        }
        return $this->componentSpecification;
    }

    public function getOrchestrationJobId(): ?string
    {
        return $this->data['orchestrationJobId'] ?? null;
    }
}

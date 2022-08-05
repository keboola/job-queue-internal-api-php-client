<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use DateTimeImmutable;
use DateTimeZone;
use JsonSerializable;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptorInterface;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Throwable;

class Job implements JsonSerializable, JobInterface
{
    public const MODE_RUN = 'run';
    public const MODE_DEBUG = 'debug';
    public const MODE_FORCE_RUN = 'forceRun';

    public const STATUS_CANCELLED = 'cancelled';
    public const STATUS_CREATED = 'created';
    public const STATUS_ERROR = 'error';
    public const STATUS_PROCESSING = 'processing';
    public const STATUS_SUCCESS = 'success';
    public const STATUS_TERMINATED = 'terminated';
    public const STATUS_TERMINATING = 'terminating';
    public const STATUS_WAITING = 'waiting';
    public const STATUS_WARNING = 'warning';

    public const STATUSES_ALL = [
        self::STATUS_CANCELLED,
        self::STATUS_CREATED,
        self::STATUS_ERROR,
        self::STATUS_PROCESSING,
        self::STATUS_SUCCESS,
        self::STATUS_TERMINATED,
        self::STATUS_TERMINATING,
        self::STATUS_WAITING,
        self::STATUS_WARNING,
    ];

    public const STATUSES_FINISHED = [
        self::STATUS_SUCCESS,
        self::STATUS_WARNING,
        self::STATUS_ERROR,
        self::STATUS_CANCELLED,
        self::STATUS_TERMINATED,
    ];

    public const STATUSES_KILLABLE = [
        self::STATUS_CREATED,
        self::STATUS_WAITING,
        self::STATUS_PROCESSING,
    ];

    public const DESIRED_STATUS_PROCESSING = 'processing';
    public const DESIRED_STATUS_TERMINATING = 'terminating';

    public const DESIRED_STATUSES_ALL = [
        self::DESIRED_STATUS_PROCESSING,
        self::DESIRED_STATUS_TERMINATING,
    ];

    public const TYPE_STANDARD = 'standard';
    public const TYPE_ROW_CONTAINER = 'container';
    public const TYPE_PHASE_CONTAINER = 'phaseContainer';
    public const TYPE_ORCHESTRATION_CONTAINER = 'orchestrationContainer';

    public const TYPES_ALL = [
        self::TYPE_STANDARD,
        self::TYPE_ROW_CONTAINER,
        self::TYPE_PHASE_CONTAINER,
        self::TYPE_ORCHESTRATION_CONTAINER,
    ];

    public const PARALLELISM_INFINITY = 'infinity';

    public static function getAllowedParallelismValues(): array
    {
        $intValues = array_map(
            fn ($item) => (string) $item,
            range(0, 100)
        );
        return array_merge($intValues, ['infinity', null]);
    }

    private JobObjectEncryptorInterface $objectEncryptor;
    private StorageClientPlainFactory $storageClientFactory;
    private array $data;

    private ?DateTimeImmutable $endTime;
    private ?DateTimeImmutable $startTime;
    private ?string $tokenDecrypted = null;
    private ?array $configDataDecrypted = null;
    private ?ComponentSpecification $componentSpecification;

    public function __construct(
        JobObjectEncryptorInterface $objectEncryptor,
        StorageClientPlainFactory $storageClientFactory,
        array $data
    ) {
        $this->objectEncryptor = $objectEncryptor;
        $this->storageClientFactory = $storageClientFactory;
        $this->data = $data;

        $this->data['isFinished'] = in_array($this->getStatus(), self::STATUSES_FINISHED);
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
        return $this->data['type'] ?? self::TYPE_STANDARD;
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
            $this->getConfigId()
        );
    }

    public function getConfigDataDecrypted(): array
    {
        return $this->configDataDecrypted ??= $this->objectEncryptor->decrypt(
            $this->getConfigData(),
            $this->getComponentId(),
            $this->getProjectId(),
            $this->getConfigId()
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

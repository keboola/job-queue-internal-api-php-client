<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use InvalidArgumentException;
use JsonSerializable;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Backend;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Executor;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\PermissionChecker\BranchType;

class PlainJob implements JsonSerializable, PlainJobInterface
{
    private ?DateTimeImmutable $endTime = null;
    private ?DateTimeImmutable $startTime = null;
    private ?DateTimeImmutable $delayedStartTime = null;
    private ?DateTimeImmutable $createdTime = null;

    public function __construct(private array $data)
    {
        if (!isset($data['branchType'])) {
            throw new InvalidArgumentException('Missing required parameter "branchType"');
        }

        $this->data['isFinished'] = in_array($this->getStatus(), PlainJobInterface::STATUSES_FINISHED);
        $this->data['parentRunId'] = $this->getParentRunId();

        if (!empty($this->data['startTime'])) {
            $this->startTime = new DateTimeImmutable($this->data['startTime'], new DateTimeZone('utc'));
            $this->data['startTime'] = $this->startTime->format('c');
        }

        if (!empty($this->data['endTime'])) {
            $this->endTime = new DateTimeImmutable($this->data['endTime'], new DateTimeZone('utc'));
            $this->data['endTime'] = $this->endTime->format('c');
        }
        if (!empty($this->data['createdTime'])) {
            $this->createdTime = new DateTimeImmutable($this->data['createdTime'], new DateTimeZone('utc'));
            $this->data['createdTime'] = $this->createdTime->format('c');
        }

        $this->initializeDelayedStartTime();
    }

    private function initializeDelayedStartTime(): void
    {
        if (!empty($this->data['delayedStartTime'])) {
            $this->delayedStartTime = new DateTimeImmutable($this->data['delayedStartTime'], new DateTimeZone('utc'));
            $this->data['delayedStartTime'] = $this->delayedStartTime->format('c');
        } elseif (!empty($this->data['delay'])) {
            $this->delayedStartTime = new DateTimeImmutable('now', new DateTimeZone('utc'));
            $this->delayedStartTime = $this->delayedStartTime->add(
                new DateInterval('PT' . (int) $this->data['delay'] . 'S'),
            );
            $this->data['delayedStartTime'] = $this->delayedStartTime->format('c');
        }
    }

    public function getId(): string
    {
        return $this->data['id'];
    }

    public function getDeduplicationId(): ?string
    {
        return $this->data['deduplicationId'] ?? null;
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

    public function getType(): JobType
    {
        return isset($this->data['type']) ?
            JobType::from($this->data['type']) :
            JobType::STANDARD;
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

    public function getBranchType(): BranchType
    {
        return BranchType::from($this->data['branchType']);
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

    public function getOrchestrationJobId(): ?string
    {
        return $this->data['orchestrationJobId'] ?? null;
    }

    /**
     * @return non-empty-string|null
     */
    public function getOrchestrationTaskId(): ?string
    {
        return $this->data['orchestrationTaskId'] ?? null;
    }

    /**
     * @return non-empty-string|null
     */
    public function getOrchestrationPhaseId(): ?string
    {
        return $this->data['orchestrationPhaseId'] ?? null;
    }

    /**
     * @return list<non-empty-list>|null
     */
    public function getOnlyOrchestrationTaskIds(): ?array
    {
        return $this->data['onlyOrchestrationTaskIds'] ?? null;
    }

    /**
     * @return non-empty-string|null
     */
    public function getPreviousJobId(): ?string
    {
        return $this->data['previousJobId'] ?? null;
    }

    public function getRunnerId(): ?string
    {
        return $this->data['runnerId'] ?? null;
    }

    public function getCreatedTime(): ?DateTimeImmutable
    {
        return $this->createdTime;
    }

    public function getDelayedStartTime(): ?DateTimeImmutable
    {
        return $this->delayedStartTime;
    }
}

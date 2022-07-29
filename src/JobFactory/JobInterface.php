<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use DateTimeImmutable;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;

interface JobInterface
{
    public const RUN_ID_DELIMITER = '.';

    public function getId(): string;
    public function getComponentId(): string;
    public function getConfigData(): array;
    public function getConfigId(): ?string;
    public function getMode(): string;
    public function getProjectId(): string;
    public function getProjectName(): string;
    public function getDataPlaneId(): ?string;
    public function getResult(): array;
    public function getConfigRowIds(): array;
    public function getStatus(): string;
    public function getDesiredStatus(): string;
    public function getTag(): ?string;
    public function getTokenString(): string;
    public function getTokenId(): string;
    public function getTokenDescription(): string;
    public function getParentRunId(): string;
    public function getRunId(): string;
    public function isFinished(): bool;
    public function getUsageData(): array;
    public function getBackend(): Backend;
    public function getType(): string;
    public function getParallelism(): ?string;
    public function getBehavior(): Behavior;
    public function jsonSerialize(): array;
    public function getTokenDecrypted(): string;
    public function getConfigDataDecrypted(): array;
    public function getBranchId(): ?string;
    public function getVariableValuesId(): ?string;
    public function getVariableValuesData(): array;
    public function getVariableValues(): VariableValues;
    public function hasVariables(): bool;
    public function getStartTime(): ?DateTimeImmutable;
    public function getEndTime(): ?DateTimeImmutable;
    public function getDurationSeconds(): ?int;
    public function getMetrics(): JobMetrics;
    public function isInRunMode(): bool;
    public function getComponentSpecification(): ComponentSpecification;
    public function getOrchestrationJobId(): ?string;
}

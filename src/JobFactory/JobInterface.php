<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

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
    public function getResult(): array;
    public function getConfigRowId(): ?string;
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
    public function jsonSerialize(): array;
    public function getTokenDecrypted(): string;
    public function getConfigDataDecrypted(): array;
    public function isLegacyComponent(): bool;
    public function getEncryptorFactory(): ObjectEncryptorFactory;
}

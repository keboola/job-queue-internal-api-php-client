<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use DateTimeImmutable;
use DateTimeZone;
use JsonSerializable;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Throwable;

class Job implements JsonSerializable, JobInterface
{
    private array $data;
    private ObjectEncryptorFactory $objectEncryptorFactory;
    private ?DateTimeImmutable $endTime;
    private ?DateTimeImmutable $startTime;

    public function __construct(ObjectEncryptorFactory $objectEncryptorFactory, array $data)
    {
        $this->data = $data;
        $this->data['isFinished'] = in_array($this->getStatus(), JobFactory::getFinishedStatuses());
        // it's important to clone here because we change state of the factory!
        // this is tested by JobFactoryTest::testEncryptionMultipleJobs()
        $this->objectEncryptorFactory = clone $objectEncryptorFactory;
        $this->objectEncryptorFactory->setProjectId($this->getProjectId());
        $this->objectEncryptorFactory->setComponentId($this->getComponentId());
        $this->objectEncryptorFactory->setConfigurationId($this->getConfigId());
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
        return $this->data['type'];
    }

    public function getParallelism(): ?string
    {
        return $this->data['parallelism'];
    }

    public function getBehavior(): Behavior
    {
        return Behavior::fromDataArray($this->data['behavior']);
    }

    public function jsonSerialize(): array
    {
        return $this->data;
    }

    public function getTokenDecrypted(): string
    {
        return $this->objectEncryptorFactory->getEncryptor(true)->decrypt($this->getTokenString());
    }

    public function getConfigDataDecrypted(): array
    {
        return $this->objectEncryptorFactory->getEncryptor()->decrypt($this->getConfigData());
    }

    public function isLegacyComponent(): bool
    {
        return empty($this->getComponentId()) || in_array($this->getComponentId(), JobFactory::getLegacyComponents());
    }

    public function getEncryptorFactory(): ObjectEncryptorFactory
    {
        return $this->objectEncryptorFactory;
    }

    public function getBranchId(): ?string
    {
        return $this->data['branchId'];
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
}

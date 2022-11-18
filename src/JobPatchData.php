<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use JsonSerializable;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\Result\JobResult;

class JobPatchData implements JsonSerializable
{
    /** @var string */
    private $status;

    /** @var string */
    private $desiredStatus;

    /** @var JobResult|null */
    private $result;

    /** @var array */
    private $usageData;

    /** @var string|null */
    private $runnerId;

    public function jsonSerialize(): array
    {
        return array_filter([
            'status' => $this->status,
            'desiredStatus' => $this->desiredStatus,
            'result' => is_null($this->result) ? null : $this->result->jsonSerialize(),
            'usageData' => $this->usageData,
            'runnerId' => $this->runnerId,
        ]);
    }

    private function validateStatus(string $status): void
    {
        if ($status && !in_array($status, JobFactory\JobInterface::STATUSES_ALL)) {
            throw new ClientException(sprintf('Invalid status: "%s".', $status));
        }
    }

    private function validateDesiredStatus(string $desiredStatus): void
    {
        $allowedDesiredStatuses = [
            JobFactory\JobInterface::DESIRED_STATUS_TERMINATING,
            JobFactory\JobInterface::DESIRED_STATUS_PROCESSING,
        ];

        if ($desiredStatus && !in_array($desiredStatus, $allowedDesiredStatuses)) {
            throw new ClientException(sprintf('Invalid desiredStatus: "%s".', $desiredStatus));
        }
    }

    public function setStatus(string $status): JobPatchData
    {
        $this->validateStatus($status);
        $this->status = $status;
        return $this;
    }

    public function getStatus(): ?string
    {
        return $this->status;
    }

    public function setDesiredStatus(string $desiredStatus): JobPatchData
    {
        $this->validateDesiredStatus($desiredStatus);
        $this->desiredStatus = $desiredStatus;
        return $this;
    }

    public function getDesiredStatus(): ?string
    {
        return $this->desiredStatus;
    }

    public function setResult(JobResult $result): JobPatchData
    {
        $this->result = $result;
        return $this;
    }

    public function getResult(): ?JobResult
    {
        return $this->result;
    }

    public function setUsageData(array $usageData): JobPatchData
    {
        $this->usageData = $usageData;
        return $this;
    }

    public function getUsageData(): ?array
    {
        return $this->usageData;
    }

    public function setRunnerId(string $runnerId): JobPatchData
    {
        $this->runnerId = $runnerId;
        return $this;
    }

    public function getRunnerId(): ?string
    {
        return $this->runnerId;
    }
}

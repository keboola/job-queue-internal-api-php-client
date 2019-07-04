<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use JsonSerializable;

class Job implements JsonSerializable
{
    public const STATUS_CANCELLED = 'cancelled';
    public const STATUS_CREATED = 'created';
    public const STATUS_ERROR = 'error';
    public const STATUS_PROCESSING = 'processing';
    public const STATUS_SUCCESS = 'success';
    public const STATUS_TERMINATED = 'terminated';
    public const STATUS_TERMINATING = 'terminating';
    public const STATUS_WAITING = 'waiting';
    public const STATUS_WARNING = 'warning';

    /** @var array */
    private $data;

    public function __construct(array $data)
    {
        $this->data = $data;
    }

    public function getComponentId(): string
    {
        return $this->data['params']['component'];
    }

    public function getConfigData(): array
    {
        return $this->data['params']['configData'] ?? [];
    }

    public function getConfigId(): ?string
    {
        return $this->data['params']['config'] ?? null;
    }

    public function getFinishedStatuses(): array
    {
        return [self::STATUS_SUCCESS, self::STATUS_WARNING, self::STATUS_ERROR, self::STATUS_CANCELLED,
            self::STATUS_TERMINATED];
    }

    public function getId(): string
    {
        return $this->data['id'];
    }

    public function getMode(): string
    {
        return $this->data['params']['mode'];
    }

    public function getProjectId(): string
    {
        return (string) $this->data['project']['id'];
    }

    public function getResult(): array
    {
        return $this->data['result'];
    }

    public function getRowId(): ?string
    {
        return $this->data['params']['row'] ?? null;
    }

    public function getStatus(): string
    {
        return $this->data['status'];
    }

    public function getTag(): ?string
    {
        return $this->data['params']['tag'] ?? null;
    }

    public function getToken(): string
    {
        return $this->data['token']['token'];
    }

    public function isFinished(): bool
    {
        return in_array($this->data['status'], $this->getFinishedStatuses());
    }

    public function jsonSerialize(): array
    {
        return $this->data;
    }
}

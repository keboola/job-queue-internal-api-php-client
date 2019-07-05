<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use JsonSerializable;
use Keboola\JobQueueInternalClient\JobFactory;

class Job implements JsonSerializable
{
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
        return in_array($this->data['status'], JobFactory::getFinishedStatuses());
    }

    public function jsonSerialize(): array
    {
        return $this->data;
    }
}

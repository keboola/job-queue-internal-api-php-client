<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

class Job
{
    /** @var array */
    private $data;

    public function __construct(array $data)
    {
        $this->data = $data;
        if (empty($this->data['params']['component'])) {
            throw new \Exception('Invalid job data: missing params.component');
        }
        if (empty($this->data['params']['mode'])) {
            throw new \Exception('Invalid job data: missing params.mode');
        }
        if (empty($this->data['token']['token'])) {
            throw new \Exception('Invalid job data: missing token.token');
        }
        if (empty($this->data['project']['id'])) {
            throw new \Exception('Invalid job data: missing project.id');
        }
    }

    public function getId(): string
    {
        return $this->data['id'];
    }

    public function getProjectId(): string
    {
        return (string) $this->data['project']['id'];
    }

    public function getToken(): string
    {
        return $this->data['token']['token'];
    }

    public function getComponentId(): string
    {
        return $this->data['params']['component'];
    }

    public function getMode(): string
    {
        return $this->data['params']['mode'];
    }

    public function getConfigId(): ?string
    {
        return $this->data['params']['config'] ?? null;
    }

    public function getConfigData(): array
    {
        // todo check that configdata is array
        return $this->data['params']['configData'] ?? [];
    }

    public function getRowId(): ?string
    {
        // todo check that row is scalar
        return $this->data['params']['row'] ?? null;
    }
}

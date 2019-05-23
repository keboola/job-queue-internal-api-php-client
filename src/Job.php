<?php

namespace Keboola\JobQueueInternalClient;

class Job
{
    /** @var array */
    private $data;

    public function __construct(array $data)
    {
        $this->data = $data;
    }

    public function getId(): string
    {
        return $this->data['id'];
    }

    public function getProjectId(): string
    {
        return $this->data['project']['id'];
    }

    public function getToken(): string
    {
        return $this->data['token']['token'];
    }

    public function getComponentId(): string
    {
        return $this->data['params']['component'];
    }

    public function getConfigId(): string
    {
        return $this->data['params']['config'];
    }

    public function getConfigData(): string
    {
        return $this->data['params']['configData'];
    }

    public function getMode(): string
    {
        return $this->data['params']['mode'];
    }

    public function getRowId(): string
    {
        return $this->data['params']['row'];
    }
}

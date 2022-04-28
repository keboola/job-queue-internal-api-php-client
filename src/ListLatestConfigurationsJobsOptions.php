<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

class ListLatestConfigurationsJobsOptions
{
    private string $projectId;
    private string $branchId;
    private ?int $offset = null;
    private ?int $limit = null;

    public function __construct(string $projectId, string $branchId = 'default')
    {
        $this->projectId = $projectId;
        $this->branchId = $branchId;
    }

    public function getQueryParameters(): array
    {
        $scalarProps = [
            'projectId' => 'projectId',
            'branchId' => 'branchId',
            'offset' => 'offset',
            'limit' => 'limit',
        ];

        $parameters = [];
        foreach ($scalarProps as $propName => $paramName) {
            if (!empty($this->$propName)) {
                $parameters[] = $paramName . '=' . urlencode((string) $this->$propName);
            }
        }

        return $parameters;
    }

    public function getProjectId(): string
    {
        return $this->projectId;
    }

    public function setProjectId(string $projectId): self
    {
        $this->projectId = $projectId;
        return $this;
    }

    public function getOffset(): ?int
    {
        return $this->offset;
    }

    public function setOffset(?int $offset): self
    {
        $this->offset = $offset;
        return $this;
    }

    public function getLimit(): ?int
    {
        return $this->limit;
    }

    public function setLimit(?int $limit): self
    {
        $this->limit = $limit;
        return $this;
    }

    public function getBranchId(): ?string
    {
        return $this->branchId;
    }

    public function setBranchId(?string $branchId): self
    {
        $this->branchId = $branchId;
        return $this;
    }
}

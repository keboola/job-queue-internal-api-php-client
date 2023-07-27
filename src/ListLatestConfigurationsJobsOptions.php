<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

class ListLatestConfigurationsJobsOptions
{
    private ?int $offset = null;
    private ?int $limit = null;

    public const BRANCH_DEFAULT_VALUE = 'default';

    public function __construct(
        private string $projectId,
        private string $branchId = self::BRANCH_DEFAULT_VALUE,
        private bool $isDefaultBranch = false,
    ) {
    }

    public function getQueryParameters(): array
    {
        $scalarProps = [
            'projectId' => 'projectId',
            'branchId' => 'branchId',
            'offset' => 'offset',
            'limit' => 'limit',
        ];
        $boolProps = [
            'isDefaultBranch' => 'isDefaultBranch',
        ];

        $parameters = [];
        foreach ($scalarProps as $propName => $paramName) {
            if (!empty($this->$propName)) {
                $parameters[] = $paramName . '=' . urlencode((string) $this->$propName);
            }
        }
        foreach ($boolProps as $propName => $paramName) {
            if (isset($this->$propName)) {
                $parameters[] = $paramName . '=' . ($this->$propName ? '1' : '0');
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

    public function isDefaultBranch(): bool
    {
        return $this->isDefaultBranch;
    }

    public function setBranchId(string $branchId, bool $isDefaultBranch): self
    {
        $this->branchId = $branchId;
        $this->isDefaultBranch = $isDefaultBranch;
        return $this;
    }
}

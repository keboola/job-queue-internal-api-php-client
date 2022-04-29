<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\ClientException;

class ListConfigurationsJobsOptions
{
    public const SORT_ORDER_ASC = 'asc';
    public const SORT_ORDER_DESC = 'desc';
    private const VALID_SORT_ORDER = [self::SORT_ORDER_ASC, self::SORT_ORDER_DESC];

    /** @var array<string> */
    private array $configIds;
    private ?int $jobsPerConfig = null;
    private ?string $projectId = null;
    private ?int $offset = null;
    private ?int $limit = null;
    private ?string $sortBy = null;
    private ?string $sortOrder = null;
    private ?string $branchId = null;
    private ?string $type = null;
    private ?string $componentId;

    public function __construct(array $configIds, ?string $componentId = null)
    {
        $allConfigIdsAreString = array_reduce($configIds, fn($valid, $item) => $valid && is_string($item), true);
        if (!$allConfigIdsAreString) {
            throw new ClientException('All configuration IDs must be strings');
        }

        $this->configIds = $configIds;
        $this->componentId = $componentId;
    }

    public function getQueryParameters(): array
    {
        $arrayableProps = [
            'configIds' => 'configId',
        ];

        $scalarProps = [
            'jobsPerConfig' => 'jobsPerConfiguration',
            'projectId' => 'projectId',
            'offset' => 'offset',
            'limit' => 'limit',
            'sortBy' => 'sortBy',
            'sortOrder' => 'sortOrder',
            'branchId' => 'branchId',
            'componentId' => 'componentId',
            'type' => 'type',
        ];

        $parameters = [];
        foreach ($arrayableProps as $propName => $paramName) {
            if (!empty($this->$propName)) {
                foreach ($this->$propName as $value) {
                    $parameters[] = $paramName . '[]=' . urlencode((string) $value);
                }
            }
        }
        foreach ($scalarProps as $propName => $paramName) {
            if (!empty($this->$propName)) {
                $parameters[] = $paramName . '=' . urlencode((string) $this->$propName);
            }
        }

        return $parameters;
    }

    public function getConfigIds(): array
    {
        return $this->configIds;
    }

    public function getJobsPerConfig(): ?int
    {
        return $this->jobsPerConfig;
    }

    public function setJobsPerConfig(?int $jobsPerConfig): self
    {
        $this->jobsPerConfig = $jobsPerConfig;
        return $this;
    }

    public function getProjectId(): ?string
    {
        return $this->projectId;
    }

    public function setProjectId(?string $projectId): self
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

    public function getSortBy(): ?string
    {
        return $this->sortBy;
    }

    public function getSortOrder(): ?string
    {
        return $this->sortOrder;
    }

    public function setSort(?string $sortBy, string $sortOrder = self::SORT_ORDER_ASC): self
    {
        if (!in_array($sortOrder, self::VALID_SORT_ORDER, true)) {
            throw new ClientException(sprintf(
                'Invalid sort order "%s", expected one of: %s',
                $sortOrder,
                implode(', ', self::VALID_SORT_ORDER)
            ));
        }

        $this->sortBy = $sortBy;
        $this->sortOrder = $sortBy === null ? null : $sortOrder;
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

    public function getComponentId(): ?string
    {
        return $this->componentId;
    }

    public function setType(string $type): self
    {
        $this->type = $type;
        return $this;
    }

    public function getType(): ?string
    {
        return $this->type;
    }
}

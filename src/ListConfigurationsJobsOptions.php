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

    public function __construct(array $configIds)
    {
        $allConfigIdsAreString = array_reduce($configIds, fn($valid, $item) => $valid && is_string($item), true);
        if (!$allConfigIdsAreString) {
            throw new ClientException('All configuration IDs must be strings');
        }

        $this->configIds = $configIds;
    }

    public function getQueryParameters(): array
    {
        $params = [
            'configId' => $this->configIds,
            'jobsPerConfiguration' => $this->jobsPerConfig,
            'projectId' => $this->projectId,
            'offset' => $this->offset,
            'limit' => $this->limit,
            'sortBy' => $this->sortBy,
            'sortOrder' => $this->sortOrder,
        ];

        return array_filter($params, function ($v) {
            return $v !== null;
        });
    }

    public function getConfigIds(): array
    {
        return $this->configIds;
    }

    public function getJobsPerConfig(): ?int
    {
        return $this->jobsPerConfig;
    }

    public function setJobsPerConfig(?int $jobsPerConfig): ListConfigurationsJobsOptions
    {
        $this->jobsPerConfig = $jobsPerConfig;
        return $this;
    }

    public function getProjectId(): ?string
    {
        return $this->projectId;
    }

    public function setProjectId(?string $projectId): ListConfigurationsJobsOptions
    {
        $this->projectId = $projectId;
        return $this;
    }

    public function getOffset(): ?int
    {
        return $this->offset;
    }

    public function setOffset(?int $offset): ListConfigurationsJobsOptions
    {
        $this->offset = $offset;
        return $this;
    }

    public function getLimit(): ?int
    {
        return $this->limit;
    }

    public function setLimit(?int $limit): ListConfigurationsJobsOptions
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

    public function setSort(?string $sortBy, string $sortOrder = self::SORT_ORDER_ASC): ListConfigurationsJobsOptions
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
}

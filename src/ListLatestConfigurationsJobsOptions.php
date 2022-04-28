<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\ClientException;

class ListLatestConfigurationsJobsOptions
{
    public const SORT_ORDER_ASC = 'asc';
    public const SORT_ORDER_DESC = 'desc';
    private const VALID_SORT_ORDER = [self::SORT_ORDER_ASC, self::SORT_ORDER_DESC];

    private string $projectId;
    private ?string $branchId = null;
    private ?string $type = null;
    private ?int $offset = null;
    private ?int $limit = null;
    private ?string $sortBy = null;
    private ?string $sortOrder = null;

    public function __construct(string $projectId)
    {
        $this->projectId = $projectId;
    }

    public function getQueryParameters(): array
    {
        $scalarProps = [
            'projectId' => 'projectId',
            'offset' => 'offset',
            'limit' => 'limit',
            'sortBy' => 'sortBy',
            'sortOrder' => 'sortOrder',
            'branchId' => 'branchId',
            'type' => 'type',
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

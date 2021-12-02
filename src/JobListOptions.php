<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use DateTimeInterface;
use Keboola\JobQueueInternalClient\Exception\ClientException;

class JobListOptions
{
    private array $ids;
    private array $runIds;
    private array $branchIds;
    private array $tokenIds;
    private array $tokenDescriptions;
    private array $components;
    private array $configs;
    private array $configRowIds;
    private array $modes;
    private array $projects;
    private array $statuses;
    private DateTimeInterface $startTimeFrom;
    private DateTimeInterface $startTimeTo;
    private DateTimeInterface $createdTimeFrom;
    private DateTimeInterface $createdTimeTo;
    private DateTimeInterface $endTimeFrom;
    private DateTimeInterface $endTimeTo;
    private int $durationSecondsFrom;
    private int $durationSecondsTo;
    private int $offset = 0;
    private int $limit = 100;
    private string $sortBy;
    private string $sortOrder;
    private ?string $parentRunId = null;
    private string $type;

    /** @var string */
    public const SORT_ORDER_ASC = 'asc';

    /** @var string */
    public const SORT_ORDER_DESC = 'desc';

    public function getQueryParameters(): array
    {
        $arrayableProps = [
            'ids' => 'id',
            'runIds' => 'runId',
            'branchIds' => 'branchId',
            'tokenIds' => 'tokenId',
            'tokenDescriptions' => 'tokenDescription',
            'components' => 'componentId',
            'configs' => 'configId',
            'configRowIds' => 'configRowIds',
            'modes' => 'mode',
            'projects' => 'projectId',
            'statuses' => 'status',
        ];
        $scalarProps = [
            'durationSecondsFrom' => 'durationSecondsFrom',
            'durationSecondsTo' => 'durationSecondsTo',
            'offset' => 'offset',
            'limit' => 'limit',
            'sortBy' => 'sortBy',
            'sortOrder' => 'sortOrder',
            'type' => 'type',
        ];
        $scalarPropsWithEmptyValueAllowed = [
            'parentRunId' => 'parentRunId',
        ];
        $dateTimeProps = [
            'startTimeFrom' => 'startTimeFrom',
            'startTimeTo' => 'startTimeTo',
            'createdTimeFrom' => 'createdTimeFrom',
            'createdTimeTo' => 'createdTimeTo',
            'endTimeFrom' => 'endTimeFrom',
            'endTimeTo' => 'endTimeTo',
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
        foreach ($scalarPropsWithEmptyValueAllowed as $propName => $paramName) {
            if (isset($this->$propName)) {
                $parameters[] = $paramName . '=' . urlencode((string) $this->$propName);
            }
        }
        foreach ($dateTimeProps as $propName => $paramName) {
            if (!empty($this->$propName)) {
                $parameters[] = $paramName . '=' . urlencode((string) $this->$propName->format('c'));
            }
        }

        return $parameters;
    }

    public function getIds(): array
    {
        return $this->ids;
    }

    public function setIds(array $values): JobListOptions
    {
        $this->ids = $values;
        return $this;
    }

    public function getRunIds(): array
    {
        return $this->runIds;
    }

    public function setRunIds(array $values): JobListOptions
    {
        $this->runIds = $values;
        return $this;
    }

    public function getBranchIds(): array
    {
        return $this->branchIds;
    }

    public function setBranchIds(array $values): JobListOptions
    {
        $this->branchIds = $values;
        return $this;
    }

    public function getTokenIds(): array
    {
        return $this->tokenIds;
    }

    public function setTokenIds(array $values): JobListOptions
    {
        $this->tokenIds = $values;
        return $this;
    }

    public function getTokenDescriptions(): array
    {
        return $this->tokenDescriptions;
    }

    public function setTokenDescriptions(array $values): JobListOptions
    {
        $this->tokenDescriptions = $values;
        return $this;
    }

    public function getComponents(): array
    {
        return $this->components;
    }

    public function setComponents(array $values): JobListOptions
    {
        $this->components = $values;
        return $this;
    }

    public function getConfigs(): array
    {
        return $this->configs;
    }

    public function setConfigs(array $values): JobListOptions
    {
        $this->configs = $values;
        return $this;
    }

    public function getConfigRowIds(): array
    {
        return $this->configRowIds;
    }

    public function setConfigRowIds(array $values): JobListOptions
    {
        $this->configRowIds = $values;
        return $this;
    }

    public function getModes(): array
    {
        return $this->modes;
    }

    public function setModes(array $values): JobListOptions
    {
        $this->modes = $values;
        return $this;
    }

    public function getProjects(): array
    {
        return $this->projects;
    }

    public function setProjects(array $values): JobListOptions
    {
        $this->projects = $values;
        return $this;
    }

    public function getStatuses(): array
    {
        return $this->statuses;
    }

    public function setStatuses(array $values): JobListOptions
    {
        $this->statuses = $values;
        return $this;
    }

    public function setDurationSecondsFrom(int $value): JobListOptions
    {
        $this->durationSecondsFrom = $value;
        return $this;
    }

    public function getDurationSecondsFrom(): int
    {
        return $this->durationSecondsFrom;
    }

    public function setDurationSecondsTo(int $value): JobListOptions
    {
        $this->durationSecondsTo = $value;
        return $this;
    }

    public function getDurationSecondsTo(): int
    {
        return $this->durationSecondsTo;
    }

    public function getOffset(): int
    {
        return $this->offset;
    }

    public function setOffset(int $value): JobListOptions
    {
        $this->offset = $value;
        return $this;
    }

    public function getLimit(): int
    {
        return $this->limit;
    }

    public function setLimit(int $value): JobListOptions
    {
        $this->limit = $value;
        return $this;
    }

    public function getEndTimeFrom(): DateTimeInterface
    {
        return $this->endTimeFrom;
    }

    public function setEndTimeFrom(DateTimeInterface $value): JobListOptions
    {
        $this->endTimeFrom = $value;
        return $this;
    }

    public function getEndTimeTo(): DateTimeInterface
    {
        return $this->endTimeTo;
    }

    public function setEndTimeTo(DateTimeInterface $value): JobListOptions
    {
        $this->endTimeTo = $value;
        return $this;
    }

    public function getStartTimeFrom(): DateTimeInterface
    {
        return $this->startTimeFrom;
    }

    public function setStartTimeFrom(DateTimeInterface $value): JobListOptions
    {
        $this->startTimeFrom = $value;
        return $this;
    }

    public function getStartTimeTo(): DateTimeInterface
    {
        return $this->startTimeTo;
    }

    public function setStartTimeTo(DateTimeInterface $value): JobListOptions
    {
        $this->startTimeTo = $value;
        return $this;
    }

    public function getCreatedTimeFrom(): DateTimeInterface
    {
        return $this->createdTimeFrom;
    }

    public function setCreatedTimeFrom(DateTimeInterface $value): JobListOptions
    {
        $this->createdTimeFrom = $value;
        return $this;
    }

    public function getCreatedTimeTo(): DateTimeInterface
    {
        return $this->createdTimeTo;
    }

    public function setCreatedTimeTo(DateTimeInterface $value): JobListOptions
    {
        $this->createdTimeTo = $value;
        return $this;
    }

    public function getSortBy(): string
    {
        return $this->sortBy;
    }

    public function setSortBy(string $value): JobListOptions
    {
        $this->sortBy = $value;
        return $this;
    }

    public function getSortOrder(): string
    {
        return $this->sortOrder;
    }

    public function setSortOrder(string $value): JobListOptions
    {
        $allowedValues = [self::SORT_ORDER_ASC, self::SORT_ORDER_DESC];
        if (!in_array($value, $allowedValues)) {
            throw new ClientException(
                sprintf('Allowed values for "sortOrder" are [%s].', implode(', ', $allowedValues))
            );
        }
        $this->sortOrder = $value;
        return $this;
    }

    public function getParentRunId(): ?string
    {
        return $this->parentRunId;
    }

    public function setParentRunId(?string $value): JobListOptions
    {
        $this->parentRunId = $value;
        return $this;
    }

    public function getType(): string
    {
        return $this->type;
    }

    public function setType(string $type): JobListOptions
    {
        $this->type = $type;
        return $this;
    }
}

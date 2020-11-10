<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

class JobListOptions
{
    /** @var array */
    private $ids;

    /** @var array */
    private $components;

    /** @var array */
    private $configs;

    /** @var array */
    private $modes;

    /** @var array */
    private $projects;

    /** @var array */
    private $statuses;

    /** @var string */
    private $startTimeFrom;

    /** @var string */
    private $startTimeTo;

    /** @var string */
    private $createdTimeFrom;

    /** @var string */
    private $createdTimeTo;

    /** @var string */
    private $endTimeFrom;

    /** @var string */
    private $endTimeTo;

    /** @var int */
    private $offset = 0;

    /** @var int */
    private $limit = 100;

    public function getQueryParameters(): array
    {
        $arrayableProps = ['ids' => 'id', 'components' => 'componentId', 'configs' => 'configId', 'modes' => 'mode',
            'projects' => 'projectId', 'statuses' => 'status'];
        $scalarProps = ['startTimeFrom' => 'startTimeFrom', 'startTimeTo' => 'startTimeTo',
            'createdTimeFrom' => 'createdTimeFrom', 'createdTimeTo' => 'createdTimeTo', 'endTimeFrom' => 'endTimeFrom',
            'endTimeTo' => 'endTimeTo', 'offset' => 'offset', 'limit' => 'limit'];
        $parameters = [];
        foreach ($arrayableProps as $propName => $paramName) {
            if (!empty($this->$propName)) {
                foreach ($this->$propName as $value) {
                    $parameters[] = $paramName . '[]=' . urlencode($value);
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

    public function getIds(): array
    {
        return $this->ids;
    }

    public function setIds(array $values): JobListOptions
    {
        $this->ids = $values;
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

    public function getStartTimeFrom(): string
    {
        return $this->startTimeFrom;
    }

    public function setStartTimeFrom(string $value): JobListOptions
    {
        $this->startTimeFrom = $value;
        return $this;
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

    public function getCreatedTimeFrom(): string
    {
        return $this->createdTimeFrom;
    }

    public function setCreatedTimeFrom(string $value): JobListOptions
    {
        $this->createdTimeFrom = $value;
        return $this;
    }

    public function getEndTimeTo(): string
    {
        return $this->endTimeTo;
    }

    public function setEndTimeTo(string $value): JobListOptions
    {
        $this->endTimeTo = $value;
        return $this;
    }

    public function getEndTimeFrom(): string
    {
        return $this->endTimeFrom;
    }

    public function setEndTimeFrom(string $value): JobListOptions
    {
        $this->endTimeFrom = $value;
        return $this;
    }

    public function getStartTimeTo(): string
    {
        return $this->startTimeTo;
    }

    public function setStartTimeTo(string $value): JobListOptions
    {
        $this->startTimeTo = $value;
        return $this;
    }

    public function getCreatedTimeTo(): string
    {
        return $this->createdTimeTo;
    }

    public function setCreatedTimeTo(string $value): JobListOptions
    {
        $this->createdTimeTo = $value;
        return $this;
    }
}

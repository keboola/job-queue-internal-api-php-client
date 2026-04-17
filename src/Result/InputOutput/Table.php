<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result\InputOutput;

use JsonSerializable;

class Table implements JsonSerializable
{
    private string $id;
    private string $name;
    private string $displayName;
    private ColumnCollection $columns;

    /** @var array<string, int|string|null> */
    private array $metrics;

    /** @param array<string, int|string|null> $metrics */
    public function __construct(
        string $id,
        string $name,
        string $displayName,
        ColumnCollection $columns,
        array $metrics = [],
    ) {
        $this->id = $id;
        $this->name = $name;
        $this->displayName = $displayName;
        $this->columns = $columns;
        $this->metrics = $metrics;
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getDisplayName(): string
    {
        return $this->displayName;
    }

    public function getColumns(): ColumnCollection
    {
        return $this->columns;
    }

    /** @return array<string, int|string|null> */
    public function getMetrics(): array
    {
        return $this->metrics;
    }

    public function jsonSerialize(): array
    {
        $result = [
            'id' => $this->id,
            'name' => $this->name,
            'displayName' => $this->displayName,
            'columns' => $this->columns->jsonSerialize(),
        ];

        $metrics = [];
        foreach ($this->metrics as $key => $value) {
            if ($value !== null) {
                $metrics[] = ['name' => $key, 'value' => $value];
            }
        }
        if ($metrics !== []) {
            $result['metrics'] = $metrics;
        }

        return $result;
    }
}

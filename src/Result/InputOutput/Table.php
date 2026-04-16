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
    private array $variables;

    /** @param array<string, int|string|null> $variables */
    public function __construct(
        string $id,
        string $name,
        string $displayName,
        ColumnCollection $columns,
        array $variables = [],
    ) {
        $this->id = $id;
        $this->name = $name;
        $this->displayName = $displayName;
        $this->columns = $columns;
        $this->variables = $variables;
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
    public function getVariables(): array
    {
        return $this->variables;
    }

    public function jsonSerialize(): array
    {
        $result = [
            'id' => $this->id,
            'name' => $this->name,
            'displayName' => $this->displayName,
            'columns' => $this->columns->jsonSerialize(),
        ];

        $variables = [];
        foreach ($this->variables as $key => $value) {
            if ($value !== null) {
                $variables[] = ['name' => $key, 'value' => $value];
            }
        }
        if ($variables !== []) {
            $result['variables'] = $variables;
        }

        return $result;
    }
}

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

    public function __construct(
        string $id,
        string $name,
        string $displayName,
        ColumnCollection $columns,
    ) {
        $this->id = $id;
        $this->name = $name;
        $this->displayName = $displayName;
        $this->columns = $columns;
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

    public function jsonSerialize(): array
    {
        return [
            'id' => $this->id,
            'name' => $this->name,
            'displayName' => $this->displayName,
            'columns' => $this->columns->jsonSerialize(),
        ];
    }
}

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
    private ?int $importedRowsCount = null;

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

    public function setImportedRowsCount(int $importedRowsCount): self
    {
        $this->importedRowsCount = $importedRowsCount;
        return $this;
    }

    public function getImportedRowsCount(): ?int
    {
        return $this->importedRowsCount;
    }

    public function jsonSerialize(): array
    {
        $result = [
            'id' => $this->id,
            'name' => $this->name,
            'displayName' => $this->displayName,
            'columns' => $this->columns->jsonSerialize(),
        ];
        if ($this->importedRowsCount !== null) {
            $result['importedRowsCount'] = $this->importedRowsCount;
        }
        return $result;
    }
}

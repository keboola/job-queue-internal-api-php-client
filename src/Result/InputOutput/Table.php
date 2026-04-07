<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result\InputOutput;

use InvalidArgumentException;
use JsonSerializable;

class Table implements JsonSerializable
{
    private const RESERVED_KEYS = ['id', 'name', 'displayName', 'columns'];

    private string $id;
    private string $name;
    private string $displayName;
    private ColumnCollection $columns;

    /** @var array<string, int|string|null> */
    private array $genericVariables;

    /** @param array<string, int|string|null> $genericVariables */
    public function __construct(
        string $id,
        string $name,
        string $displayName,
        ColumnCollection $columns,
        array $genericVariables = [],
    ) {
        $reservedConflicts = array_intersect(array_keys($genericVariables), self::RESERVED_KEYS);
        if ($reservedConflicts !== []) {
            throw new InvalidArgumentException(sprintf(
                'Generic variables must not use reserved keys: %s',
                implode(', ', $reservedConflicts),
            ));
        }

        $this->id = $id;
        $this->name = $name;
        $this->displayName = $displayName;
        $this->columns = $columns;
        $this->genericVariables = $genericVariables;
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
    public function getGenericVariables(): array
    {
        return $this->genericVariables;
    }

    public function jsonSerialize(): array
    {
        $result = [
            'id' => $this->id,
            'name' => $this->name,
            'displayName' => $this->displayName,
            'columns' => $this->columns->jsonSerialize(),
        ];
        foreach ($this->genericVariables as $key => $value) {
            if ($value !== null) {
                $result[$key] = $value;
            }
        }
        return $result;
    }
}

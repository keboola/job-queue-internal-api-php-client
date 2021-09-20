<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\Mapping;

use Countable;
use Generator;
use IteratorAggregate;
use JsonSerializable;

/**
 * @implements IteratorAggregate<Table>
 */
class TableCollection implements Countable, IteratorAggregate, JsonSerializable
{
    /** @var Table[] */
    private array $items = [];

    /**
     * @return Generator<int, Table>
     */
    public function getIterator(): Generator
    {
        foreach ($this->items as $item) {
            yield $item;
        }
    }

    public function count(): int
    {
        return count($this->items);
    }

    public function jsonSerialize(): array
    {
        return array_map(function (Table $table) {
            return $table->jsonSerialize();
        }, $this->items);
    }

    public function addTable(Table $table): self
    {
        $this->items[] = $table;
        return $this;
    }
}

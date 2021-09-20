<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\Mapping;

use Countable;
use Generator;
use IteratorAggregate;

/**
 * @implements IteratorAggregate<Column>
 */
class ColumnCollection implements IteratorAggregate, Countable
{
    /** @var Column[] */
    private array $items = [];

    /**
     * @return Generator<int, Column>
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
        return array_map(function (Column $column) {
            return $column->jsonSerialize();
        }, $this->items);
    }

    public function addColumn(Column $column): self
    {
        $this->items[] = $column;
        return $this;
    }
}

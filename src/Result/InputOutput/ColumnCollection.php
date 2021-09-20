<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result\InputOutput;

use Countable;
use Generator;
use IteratorAggregate;
use JsonSerializable;
use Keboola\JobQueueInternalClient\Result\InputOutput\Column;
use function Keboola\JobQueueInternalClient\JobFactory\Mapping\count;

/**
 * @implements IteratorAggregate<Column>
 */
class ColumnCollection implements Countable, IteratorAggregate, JsonSerializable
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

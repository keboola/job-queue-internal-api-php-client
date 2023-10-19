<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result\Variable;

use Countable;
use Generator;
use IteratorAggregate;
use JsonSerializable;

/**
 * @implements IteratorAggregate<Variable>
 */
class VariableCollection implements Countable, IteratorAggregate, JsonSerializable
{
    /** @var Variable[] */
    private array $items = [];

    /**
     * @return Generator<int, Variable>
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
        return array_map(function (Variable $variable) {
            return $variable->jsonSerialize();
        }, $this->items);
    }

    public function addVariable(Variable $variable): self
    {
        $this->items[] = $variable;
        return $this;
    }
}

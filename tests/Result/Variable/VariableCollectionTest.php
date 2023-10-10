<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result\InputOutput;

use Keboola\JobQueueInternalClient\Result\Variable\Variable;
use Keboola\JobQueueInternalClient\Result\Variable\VariableCollection;
use PHPUnit\Framework\TestCase;

class VariableCollectionTest extends TestCase
{
    public function testCreate(): void
    {
        $collection = new VariableCollection();

        self::assertSame(0, $collection->count());

        $variable = new Variable('foo', 'bar');
        $collection->addVariable($variable);

        self::assertSame(1, $collection->count());
        self::assertSame(
            [
                $variable,
            ],
            iterator_to_array($collection->getIterator()),
        );
        self::assertSame([
            [
                'name' => 'foo',
                'value' => 'bar',
            ],
        ], $collection->jsonSerialize());
    }
}

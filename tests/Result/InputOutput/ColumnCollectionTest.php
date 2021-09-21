<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result\InputOutput;

use Keboola\JobQueueInternalClient\Result\InputOutput\Column;
use Keboola\JobQueueInternalClient\Result\InputOutput\ColumnCollection;
use PHPUnit\Framework\TestCase;

class ColumnCollectionTest extends TestCase
{
    public function testCreate(): void
    {
        $collection = new ColumnCollection();

        self::assertSame(0, $collection->count());

        $column1 = new Column('created');
        $column2 = new Column('id');

        $collection
            ->addColumn($column1)
            ->addColumn($column2)
        ;

        self::assertSame(2, $collection->count());

        $iterator = iterator_to_array($collection->getIterator());
        self::assertSame(2, count($iterator));
        self::assertSame($column1, $iterator[0]);
        self::assertSame($column2, $iterator[1]);

        self::assertSame([
            [
                'name' => 'created',
            ],
            [
                'name' => 'id',
            ],
        ], $collection->jsonSerialize());
    }
}

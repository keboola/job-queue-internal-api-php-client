<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\Mapping;

use Keboola\JobQueueInternalClient\Result\InputOutput\Column;
use Keboola\JobQueueInternalClient\Result\InputOutput\ColumnCollection;
use PHPUnit\Framework\TestCase;

class ColumnCollectionTest extends TestCase
{
    public function testCreate(): void
    {
        $collection = new ColumnCollection();

        self::assertSame(0, $collection->count());

        $column1 = Column::fromDataArray([
            'name' => 'created',
        ]);

        $column2 = Column::fromDataArray([
            'name' => 'id',
        ]);

        $collection
            ->addColumn($column1)
            ->addColumn($column2)
        ;

        self::assertSame(2, $collection->count());

        $itterator = iterator_to_array($collection->getIterator());
        self::assertSame(2, count($itterator));
        self::assertSame($column1, $itterator[0]);
        self::assertSame($column2, $itterator[1]);

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

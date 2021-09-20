<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\Mapping;

use Keboola\JobQueueInternalClient\JobFactory\Mapping\Column;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\ColumnCollection;
use PHPUnit\Framework\TestCase;

class ColumnCollectionTest extends TestCase
{
    public function testCreate(): void
    {
        $collection = new ColumnCollection();

        self::assertSame(0, $collection->count());

        $column1 = Column::fromDataArray([
            'id' => 'in.c-bucket.table.created',
            'name' => 'created',
            'displayName' => 'Created date',
        ]);

        $column2 = Column::fromDataArray([
            'id' => 'in.c-bucket.table.id',
            'name' => 'id',
            'displayName' => 'ID',
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
                'id' => 'in.c-bucket.table.created',
                'name' => 'created',
                'displayName' => 'Created date',
            ],
            [
                'id' => 'in.c-bucket.table.id',
                'name' => 'id',
                'displayName' => 'ID',
            ],
        ], $collection->jsonSerialize());
    }
}

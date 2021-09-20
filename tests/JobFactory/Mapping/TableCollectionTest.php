<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\Mapping;

use Keboola\JobQueueInternalClient\JobFactory\Mapping\Column;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\ColumnCollection;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\Table;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\TableCollection;
use PHPUnit\Framework\TestCase;

class TableCollectionTest extends TestCase
{
    public function testCreate(): void
    {
        $collection = new TableCollection();

        self::assertSame(0, $collection->count());

        $table = new Table(
            'in.c-bucket.table',
            'table',
            'MyTable',
            (new ColumnCollection())->addColumn(
                Column::fromDataArray([
                    'name' => 'id',
                ])
            )
        );

        $collection->addTable($table);

        self::assertSame(1, $collection->count());

        self::assertSame([
            [
                'id' => 'in.c-bucket.table',
                'name' => 'table',
                'displayName' => 'MyTable',
                'columns' => [
                    [
                        'name' => 'id',
                    ],
                ],
            ],
        ], $collection->jsonSerialize());
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result\InputOutput;

use Keboola\JobQueueInternalClient\Result\InputOutput\Column;
use Keboola\JobQueueInternalClient\Result\InputOutput\ColumnCollection;
use Keboola\JobQueueInternalClient\Result\InputOutput\Table;
use Keboola\JobQueueInternalClient\Result\InputOutput\TableCollection;
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
            (new ColumnCollection())->addColumn(new Column('id')),
        );

        $collection->addTable($table);

        self::assertSame(1, $collection->count());
        self::assertEquals(
            [
                new Table(
                    'in.c-bucket.table',
                    'table',
                    'MyTable',
                    (new ColumnCollection())->addColumn(new Column('id')),
                ),
            ],
            iterator_to_array($collection->getIterator()),
        );
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

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result\InputOutput;

use Keboola\JobQueueInternalClient\Result\InputOutput\Column;
use Keboola\JobQueueInternalClient\Result\InputOutput\ColumnCollection;
use Keboola\JobQueueInternalClient\Result\InputOutput\Table;
use PHPUnit\Framework\TestCase;

class TableTest extends TestCase
{
    public function testCreate(): void
    {
        $collection = (new ColumnCollection())->addColumn(new Column('created'));
        $table = new Table('in.c-bucket.table', 'myTable', 'Test table', $collection);

        self::assertSame('in.c-bucket.table', $table->getId());
        self::assertSame('myTable', $table->getName());
        self::assertSame('Test table', $table->getDisplayName());
        self::assertSame(1, $table->getColumns()->count());
        self::assertNull($table->getRowsCount());

        self::assertSame([
            'id' => 'in.c-bucket.table',
            'name' => 'myTable',
            'displayName' => 'Test table',
            'columns' => [
                [
                    'name' => 'created',
                ],
            ],
        ], $table->jsonSerialize());
    }

    public function testRowsCount(): void
    {
        $collection = (new ColumnCollection())->addColumn(new Column('id'));
        $table = new Table('out.c-bucket.orders', 'orders', 'Orders', $collection);
        $table->setRowsCount(100);

        self::assertSame(100, $table->getRowsCount());
        self::assertSame([
            'id' => 'out.c-bucket.orders',
            'name' => 'orders',
            'displayName' => 'Orders',
            'columns' => [['name' => 'id']],
            'rowsCount' => 100,
        ], $table->jsonSerialize());
    }
}

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

    public function testTableVariables(): void
    {
        $collection = new ColumnCollection();
        $table = new Table(
            'out.c-bucket.orders',
            'orders',
            'Orders',
            $collection,
            ['importedRowsCount' => 123, 'someString' => 'hello'],
        );

        self::assertSame(['importedRowsCount' => 123, 'someString' => 'hello'], $table->getMetrics());
        self::assertSame([
            'id' => 'out.c-bucket.orders',
            'name' => 'orders',
            'displayName' => 'Orders',
            'columns' => [],
            'metrics' => [
                ['name' => 'importedRowsCount', 'value' => 123],
                ['name' => 'someString', 'value' => 'hello'],
            ],
        ], $table->jsonSerialize());
    }

    public function testNullTableVariablesAreOmittedFromJson(): void
    {
        $collection = new ColumnCollection();
        $table = new Table(
            'out.c-bucket.orders',
            'orders',
            'Orders',
            $collection,
            ['importedRowsCount' => null, 'someString' => 'hello'],
        );

        self::assertSame([
            'id' => 'out.c-bucket.orders',
            'name' => 'orders',
            'displayName' => 'Orders',
            'columns' => [],
            'metrics' => [
                ['name' => 'someString', 'value' => 'hello'],
            ],
        ], $table->jsonSerialize());
    }

    public function testEmptyTableVariablesDoNotAppearInJson(): void
    {
        $collection = new ColumnCollection();
        $table = new Table('out.c-bucket.orders', 'orders', 'Orders', $collection);

        self::assertSame([
            'id' => 'out.c-bucket.orders',
            'name' => 'orders',
            'displayName' => 'Orders',
            'columns' => [],
        ], $table->jsonSerialize());
    }
}

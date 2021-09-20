<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\Mapping;

use Keboola\JobQueueInternalClient\JobFactory\Mapping\Column;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\ColumnCollection;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\Table;
use PHPUnit\Framework\TestCase;

class TableTest extends TestCase
{
    public function testCreate(): void
    {
        $collection = (new ColumnCollection())->addColumn(
            Column::fromDataArray([
                'id' => 'in.c-bucket.table.created',
                'name' => 'created',
                'displayName' => 'Created date',
            ])
        );

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
                    'id' => 'in.c-bucket.table.created',
                    'name' => 'created',
                    'displayName' => 'Created date',
                ],
            ],
        ], $table->jsonSerialize());
    }
}

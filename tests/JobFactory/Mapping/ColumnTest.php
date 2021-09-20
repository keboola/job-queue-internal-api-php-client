<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\Mapping;

use Generator;
use InvalidArgumentException;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\Column;
use PHPUnit\Framework\TestCase;

class ColumnTest extends TestCase
{
    public function testCreate(): void
    {
        $column = new Column('in.c-bucket.table.created', 'created', 'Created date');

        self::assertSame('in.c-bucket.table.created', $column->getId());
        self::assertSame('created', $column->getName());
        self::assertSame('Created date', $column->getDisplayName());

        self::assertSame([
            'id' => 'in.c-bucket.table.created',
            'name' => 'created',
            'displayName' => 'Created date',
        ], $column->jsonSerialize());
    }

    public function testCreateFromArray(): void
    {
        $sourceData = [
            'id' => 'in.c-bucket.table.created',
            'name' => 'created',
            'displayName' => 'Created date',
        ];

        $column = Column::fromDataArray($sourceData);

        self::assertSame('in.c-bucket.table.created', $column->getId());
        self::assertSame('created', $column->getName());
        self::assertSame('Created date', $column->getDisplayName());

        self::assertSame($sourceData, $column->jsonSerialize());
    }

    /**
     * @dataProvider createFromInvalidArrayData
     */
    public function testCreateFromInvalidArray(array $sourceData, string $expectedErrorMessage): void
    {
        self::expectExceptionMessage($expectedErrorMessage);
        self::expectException(InvalidArgumentException::class);

        Column::fromDataArray($sourceData);
    }

    /**
     * @return Generator<array{array, string}>
     */
    public function createFromInvalidArrayData(): Generator
    {
        yield 'empty array' => [
            [],
            'Empty value or missing data for "id".',
        ];
        yield 'missing name + display name' => [
            [
                'id' => 'test',
            ],
            'Empty value or missing data for "name".',
        ];
        yield 'missing display name' => [
            [
                'id' => 'test',
                'name' => 'testName',
            ],
            'Empty value or missing data for "displayName".',
        ];
    }
}

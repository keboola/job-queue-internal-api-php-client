<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\Mapping;

use Generator;
use InvalidArgumentException;
use Keboola\JobQueueInternalClient\Result\InputOutput\Column;
use PHPUnit\Framework\TestCase;

class ColumnTest extends TestCase
{
    public function testCreate(): void
    {
        $column = new Column('created');

        self::assertSame('created', $column->getName());

        self::assertSame([
            'name' => 'created',
        ], $column->jsonSerialize());
    }

    public function testCreateFromArray(): void
    {
        $sourceData = [
            'name' => 'created',
        ];

        $column = Column::fromDataArray($sourceData);

        self::assertSame('created', $column->getName());
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
            'Empty value or missing data for "name".',
        ];
        yield 'missing name' => [
            [
                'test' => 'test',
            ],
            'Empty value or missing data for "name".',
        ];
    }
}

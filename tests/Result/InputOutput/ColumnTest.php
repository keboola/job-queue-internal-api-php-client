<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result\InputOutput;

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

        $column = new Column($sourceData['name']);

        self::assertSame('created', $column->getName());
        self::assertSame($sourceData, $column->jsonSerialize());
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result\InputOutput;

use Keboola\JobQueueInternalClient\Result\InputOutput\Column;
use Keboola\JobQueueInternalClient\Result\InputOutput\ColumnCollection;
use Keboola\JobQueueInternalClient\Result\InputOutput\Table;
use Keboola\JobQueueInternalClient\Result\Variable\Variable;
use PHPUnit\Framework\TestCase;

class VariableTest extends TestCase
{
    public function testCreate(): void
    {
        $table = new Variable('vault.foo', 'bar');

        self::assertSame('vault.foo', $table->getName());
        self::assertSame('bar', $table->getValue());

        self::assertSame([
            'name' => 'vault.foo',
            'value' => 'bar',
        ], $table->jsonSerialize());
    }
}

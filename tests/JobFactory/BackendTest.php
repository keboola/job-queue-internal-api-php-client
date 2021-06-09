<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\Backend;
use PHPUnit\Framework\TestCase;

class BackendTest extends TestCase
{

    public function typesProvider(): iterable
    {
        yield 'null' => [null];
        yield 'foo' => ['foo'];
    }

    /**
     * @dataProvider typesProvider
     */
    public function testCreate(?string $type): void
    {
        $backend = new Backend($type);
        self::assertSame($type, $backend->getType());
    }

    public function testCreateFromArray(): void
    {
        $backend = Backend::fromDataArray([
            'type' => 'foo',
        ]);

        self::assertSame('foo', $backend->getType());
    }
}

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

    /**
     * @dataProvider provideCreateFromArrayData
     */
    public function testCreateFromArray(array $data, ?string $expectedType): void
    {
        $backend = Backend::fromDataArray($data);

        self::assertSame($expectedType, $backend->getType());
    }

    public function provideCreateFromArrayData(): iterable
    {
        yield 'empty' => [[], null];
        yield 'with type' => [['type' => 'custom'], 'custom'];
    }

    /**
     * @dataProvider provideExportAsDataArrayData
     */
    public function testExportAsDataArray(Backend $backend, array $expectedResult): void
    {
        self::assertSame($expectedResult, $backend->asDataArray());
    }

    public function provideExportAsDataArrayData(): iterable
    {
        yield 'empty' => [new Backend(null), []];
        yield 'with type' => [new Backend('custom'), ['type' => 'custom']];
    }
}

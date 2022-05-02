<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\Backend;
use PHPUnit\Framework\TestCase;

class BackendTest extends TestCase
{
    public function typesProvider(): iterable
    {
        yield 'null' => [null, null];
        yield 'foo' => ['foo', null];
        yield 'containerFoo' => [null, 'foo'];
        yield 'FooBoo' => ['foo', 'boo'];
    }

    /**
     * @dataProvider typesProvider
     */
    public function testCreate(?string $type, ?string $containerType): void
    {
        $backend = new Backend($type, $containerType);
        self::assertSame($type, $backend->getType());
        self::assertSame($containerType, $backend->getContainerType());
    }

    /**
     * @dataProvider provideCreateFromArrayData
     */
    public function testCreateFromArray(
        array $data,
        ?string $expectedType,
        ?string $expectedContainerType,
        bool $expectedEmpty
    ): void {
        $backend = Backend::fromDataArray($data);

        self::assertSame($expectedType, $backend->getType());
        self::assertSame($expectedContainerType, $backend->getContainerType());
        self::assertSame($expectedEmpty, $backend->isEmpty());
    }

    public function provideCreateFromArrayData(): iterable
    {
        yield 'empty' => [[], null, null, true];
        yield 'with type' => [['type' => 'custom'], 'custom', null, false];
        yield 'with container type' => [['containerType' => 'custom'], null, 'custom', false];
        yield 'with both types' => [['type' => 'custom', 'containerType' => 'motsuc'], 'custom', 'motsuc', false];
    }

    /**
     * @dataProvider provideExportAsDataArrayData
     */
    public function testExportAsDataArray(Backend $backend, array $expectedResult): void
    {
        self::assertSame($expectedResult, $backend->toDataArray());
    }

    public function provideExportAsDataArrayData(): iterable
    {
        yield 'empty' => [
            new Backend(null, null),
            ['type' => null, 'containerType' => null],
        ];
        yield 'with type' => [
            new Backend('custom', null),
            ['type' => 'custom', 'containerType' => null],
        ];
        yield 'with container type' => [
            new Backend(null, 'custom'),
            ['type' => null, 'containerType' => 'custom'],
        ];
        yield 'with both types' => [
            new Backend('custom', 'motsuc'),
            ['type' => 'custom', 'containerType' => 'motsuc'],
        ];
    }
}

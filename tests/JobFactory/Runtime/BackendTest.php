<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\Runtime;

use Keboola\JobQueueInternalClient\JobFactory\Runtime\Backend;
use PHPUnit\Framework\TestCase;

class BackendTest extends TestCase
{
    public function typesProvider(): iterable
    {
        yield 'null' => [null, null, null];
        yield 'foo' => ['foo', null, null];
        yield 'containerFoo' => [null, 'foo', null];
        yield 'contextFoo' => [null, null, 'foo'];
        yield 'FooBooLoo' => ['foo', 'boo', 'loo'];
    }

    /**
     * @dataProvider typesProvider
     */
    public function testCreate(?string $type, ?string $containerType, ?string $context): void
    {
        $backend = new Backend($type, $containerType, $context);
        self::assertSame($type, $backend->getType());
        self::assertSame($containerType, $backend->getContainerType());
        self::assertSame($context, $backend->getContext());
    }

    /**
     * @dataProvider provideCreateFromArrayData
     */
    public function testCreateFromArray(
        array $data,
        ?string $expectedType,
        ?string $expectedContainerType,
        ?string $expectedContext,
        bool $expectedEmpty
    ): void {
        $backend = Backend::fromDataArray($data);

        self::assertSame($expectedType, $backend->getType());
        self::assertSame($expectedContainerType, $backend->getContainerType());
        self::assertSame($expectedContext, $backend->getContext());
        self::assertSame($expectedEmpty, $backend->isEmpty());
    }

    public function provideCreateFromArrayData(): iterable
    {
        yield 'empty' => [
            [],
            null,
            null,
            null,
            true,
        ];
        yield 'with type' => [
            [
                'type' => 'custom',
            ],
            'custom',
            null,
            null,
            false,
        ];
        yield 'with container type' => [
            [
                'containerType' => 'custom',
            ],
            null,
            'custom',
            null,
            false,
        ];
        yield 'with context' => [
            [
                'context' => 'wlm',
            ],
            null,
            null,
            'wlm',
            false,
        ];
        yield 'with all properties' => [
            [
                'type' => 'custom',
                'containerType' => 'motsuc',
                'context' => 'wlm',
            ],
            'custom',
            'motsuc',
            'wlm',
            false,
        ];
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
            new Backend(null, null, null),
            [
                'type' => null,
                'containerType' => null,
                'context' => null,
            ],
        ];
        yield 'with type' => [
            new Backend('custom', null, null),
            [
                'type' => 'custom',
                'containerType' => null,
                'context' => null,
            ],
        ];
        yield 'with container type' => [
            new Backend(null, 'custom', null),
            [
                'type' => null,
                'containerType' => 'custom',
                'context' => null,
            ],
        ];
        yield 'with context' => [
            new Backend(null, null, 'wml'),
            [
                'type' => null,
                'containerType' => null,
                'context' => 'wml',
            ],
        ];
        yield 'with all properties' => [
            new Backend('custom', 'motsuc', 'wml'),
            [
                'type' => 'custom',
                'containerType' => 'motsuc',
                'context' => 'wml',
            ],
        ];
    }
}

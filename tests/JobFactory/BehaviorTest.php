<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\Behavior;
use PHPUnit\Framework\TestCase;

class BehaviorTest extends TestCase
{

    public function onErrorProvider(): iterable
    {
        yield 'null' => [null];
        yield 'warning' => ['warning'];
    }

    /**
     * @dataProvider onErrorProvider
     */
    public function testCreate(?string $onError): void
    {
        $behavior = new Behavior($onError);
        self::assertSame($onError, $behavior->getOnError());
    }

    /**
     * @dataProvider provideCreateFromArrayData
     */
    public function testCreateFromArray(array $data, ?string $expected): void
    {
        $behavior = Behavior::fromDataArray($data);
        self::assertSame($expected, $behavior->getOnError());
    }

    public function provideCreateFromArrayData(): iterable
    {
        yield 'empty' => [[], null, true];
        yield 'with type' => [['onError' => 'warning'], 'warning', false];
    }

    /**
     * @dataProvider provideToDataArrayData
     */
    public function testExportAsDataArray(Behavior $behavior, array $expectedResult): void
    {
        self::assertSame($expectedResult, $behavior->toDataArray());
    }

    public function provideToDataArrayData(): iterable
    {
        yield 'empty' => [new Behavior(null), ['onError' => null]];
        yield 'with type' => [new Behavior('custom'), ['onError' => 'custom']];
    }
}

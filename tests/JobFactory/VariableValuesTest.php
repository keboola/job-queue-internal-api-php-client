<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\VariableValues;
use PHPUnit\Framework\TestCase;

class VariableValuesTest extends TestCase
{
    /**
     * @dataProvider validValuesProvider
     */
    public function testConstructValid(?string $expectedValuesId, array $expectedValuesData, bool $empty): void
    {
        $values = new VariableValues($expectedValuesId, $expectedValuesData);
        self::assertSame($expectedValuesId, $values->getValuesId());
        self::assertSame($expectedValuesData, $values->getValuesData());
        self::assertSame($empty, $values->isEmpty());
    }

    public function validValuesProvider(): iterable
    {
        yield 'empty' => [null, [], true];
        yield 'empty values' => [null, ['values' => []], true];
        yield 'id' => ['123', [], false];
        yield 'data' => [null, ['values' => ['123']], false];
    }

    public function testConstructInvalid(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Provide either "variableValuesId" or "variableValuesData", but not both.');
        new VariableValues('123', ['values' => ['123']]);
    }

    /**
     * @dataProvider arrayValuesProvider
     */
    public function testCreateFromArray(array $data, ?string $expectedValuesId, array $expectedValuesData): void
    {
        $values = VariableValues::fromDataArray($data);
        self::assertSame($expectedValuesData, $values->getValuesData());
        self::assertSame($expectedValuesId, $values->getValuesId());
    }

    public function arrayValuesProvider(): iterable
    {
        yield 'empty1' => [['variableValuesId' => null], null, []];
        yield 'empty2' => [['variableValuesId' => ''], '', []];
        yield 'id' => [['variableValuesId' => '123'], '123', []];
        yield 'data' => [['variableValuesData' => ['bar']], null, ['bar']];
    }

    /**
     * @dataProvider dataArrayProvider
     */
    public function testExportAsDataArray(VariableValues $values, array $expectedData): void
    {
        self::assertSame($expectedData, $values->asDataArray());
    }

    public function dataArrayProvider(): iterable
    {
        yield 'empty values id' => [
            new VariableValues('', []),
            ['variableValuesId' => '', 'variableValuesData' => []],
        ];
        yield 'empty values data' => [
            new VariableValues(null, []),
            ['variableValuesId' => null, 'variableValuesData' => []],
        ];
        yield 'empty values' => [
            new VariableValues(null, ['values' => []]),
            ['variableValuesId' => null, 'variableValuesData' => ['values' => []]],
        ];
        yield 'id' => [
            new VariableValues('123', []),
            ['variableValuesId' => '123', 'variableValuesData' => []],
        ];
        yield 'data' => [
            new VariableValues(null, ['values' => ['123']]),
            ['variableValuesId' => null, 'variableValuesData' => ['values' => ['123']]],
        ];
    }
}

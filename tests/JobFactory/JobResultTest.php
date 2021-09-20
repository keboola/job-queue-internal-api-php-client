<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\JobResult;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\Column;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\ColumnCollection;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\Table;
use Keboola\JobQueueInternalClient\JobFactory\Mapping\TableCollection;
use PHPUnit\Framework\TestCase;

class JobResultTest extends TestCase
{
    public function testAccessors(): void
    {
        $inputTable = new Table(
            'in.c-myBucket.myTable',
            'myTable',
            'My Table',
            (new ColumnCollection())->addColumn(new Column('street'))
        );

        $input = (new TableCollection())->addTable($inputTable);

        $outputTable = new Table(
            'out.c-myBucket.myOutput',
            'myOutputTable',
            'My Output Table',
            (new ColumnCollection())->addColumn(new Column('city'))
        );

        $output = (new TableCollection())->addTable($outputTable);

        $jobResult = new JobResult();
        $jobResult
            ->setConfigVersion('123')
            ->setMessage('test')
            ->setImages(['first', 'second'])
            ->setErrorType('application')
            ->setExceptionId('exception-12345')
            ->setInputTables($input)
            ->setOutputTables($output)
        ;
        self::assertSame('123', $jobResult->getConfigVersion());
        self::assertSame('test', $jobResult->getMessage());
        self::assertSame(['first', 'second'], $jobResult->getImages());
        self::assertSame('application', $jobResult->getErrorType());
        self::assertSame('exception-12345', $jobResult->getExceptionId());
        self::assertSame($input, $jobResult->getIntputTables());
        self::assertSame($output, $jobResult->getOutputTables());
        self::assertSame(
            [
                'message' => 'test',
                'configVersion' => '123',
                'images' => ['first', 'second'],
                'input' => [
                    'tables' => [
                        [
                            'id' => 'in.c-myBucket.myTable',
                            'name' => 'myTable',
                            'displayName' => 'My Table',
                            'columns' => [
                                [
                                    'name' => 'street',
                                ],
                            ],
                        ],
                    ],
                ],
                'output' => [
                    'tables' => [
                        [
                            'id' => 'out.c-myBucket.myOutput',
                            'name' => 'myOutputTable',
                            'displayName' => 'My Output Table',
                            'columns' => [
                                [
                                    'name' => 'city',
                                ],
                            ],
                        ],
                    ],
                ],
                'error' => [
                    'type' => 'application',
                    'exceptionId' => 'exception-12345',
                ],
            ],
            $jobResult->jsonSerialize()
        );
    }

    public function testEmptyResult(): void
    {
        $result = new JobResult();
        self::assertNull($result->getImages());
        self::assertNull($result->getErrorType());
        self::assertNull($result->getConfigVersion());
        self::assertNull($result->getExceptionId());
        self::assertNull($result->getMessage());
        self::assertNull($result->getIntputTables());
        self::assertNull($result->getOutputTables());

        self::assertSame([
            'message' => null,
            'configVersion' => null,
            'images' => null,
        ], $result->jsonSerialize());
    }

    public function testInvalidErrorType(): void
    {
        $jobResult = new JobResult();
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid error type: "boo".');
        $jobResult->setErrorType('boo');
    }
}

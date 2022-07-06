<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Result\Artifacts;
use Keboola\JobQueueInternalClient\Result\InputOutput\Column;
use Keboola\JobQueueInternalClient\Result\InputOutput\ColumnCollection;
use Keboola\JobQueueInternalClient\Result\InputOutput\Table;
use Keboola\JobQueueInternalClient\Result\InputOutput\TableCollection;
use Keboola\JobQueueInternalClient\Result\JobResult;
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
            ->setArtifacts(
                (new Artifacts())
                    ->setUploaded([
                        'storageFileId' => '12345',
                    ])
                    ->setDownloaded([
                        [
                            'storageFileId' => '12345',
                        ],
                        [
                            'storageFileId' => '12346',
                        ],
                        [
                            'storageFileId' => '12347',
                        ],
                    ])
            )
        ;
        self::assertSame('123', $jobResult->getConfigVersion());
        self::assertSame('test', $jobResult->getMessage());
        self::assertSame(['first', 'second'], $jobResult->getImages());
        self::assertSame('application', $jobResult->getErrorType());
        self::assertSame('exception-12345', $jobResult->getExceptionId());
        self::assertSame($input, $jobResult->getInputTables());
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
                'artifacts' => [
                    'uploaded' => [
                        'storageFileId' => '12345',
                    ],
                    'downloaded' => [
                        [
                            'storageFileId' => '12345',
                        ],
                        [
                            'storageFileId' => '12346',
                        ],
                        [
                            'storageFileId' => '12347',
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
        self::assertIsArray($result->getImages());
        self::assertEmpty($result->getImages());
        self::assertNull($result->getErrorType());
        self::assertNull($result->getConfigVersion());
        self::assertNull($result->getExceptionId());
        self::assertNull($result->getMessage());
        self::assertNull($result->getInputTables());
        self::assertNull($result->getOutputTables());

        self::assertSame([
            'message' => null,
            'configVersion' => null,
            'images' => [],
            'input' => [
                'tables' => [],
            ],
            'output' => [
                'tables' => [],
            ],
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

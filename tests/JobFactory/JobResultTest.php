<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\JobResult;
use PHPUnit\Framework\TestCase;

class JobResultTest extends TestCase
{
    public function testAccessors(): void
    {
        $jobResult = new JobResult();
        $jobResult
            ->setConfigVersion('123')
            ->setMessage('test')
            ->setImages(['first', 'second'])
            ->setErrorType('application')
            ->setExceptionId('exception-12345');
        self::assertSame('123', $jobResult->getConfigVersion());
        self::assertSame('test', $jobResult->getMessage());
        self::assertSame(['first', 'second'], $jobResult->getImages());
        self::assertSame('application', $jobResult->getErrorType());
        self::assertSame('exception-12345', $jobResult->getExceptionId());
        self::assertSame(
            [
                'message' => 'test',
                'configVersion' => '123',
                'images' => ['first', 'second'],
                'input' => [
                    'tables' => [],
                    'files' => [],
                ],
                'output' => [
                    'tables' => [],
                    'files' => [],
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
    }

    public function testInvalidErrorType(): void
    {
        $jobResult = new JobResult();
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid error type: "boo".');
        $jobResult->setErrorType('boo');
    }
}

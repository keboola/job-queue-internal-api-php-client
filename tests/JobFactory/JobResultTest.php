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
                'error' => [
                    'type' => 'application',
                ],
            ],
            $jobResult->jsonSerialize()
        );
    }

    public function testInvalidErrorType(): void
    {
        $jobResult = new JobResult();
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid error type: "boo".');
        $jobResult->setErrorType('boo');
    }
}

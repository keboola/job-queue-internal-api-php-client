<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Generator;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\Exception\StateTargetEqualsCurrentException;
use Keboola\JobQueueInternalClient\Exception\StateTerminalException;
use Keboola\JobQueueInternalClient\Exception\StateTransitionForbiddenException;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\GenericObjectEncryptorProvider;
use Keboola\JobQueueInternalClient\Result\JobResult;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Psr\Log\NullLogger;
use Throwable;

class ClientExceptionTest extends BaseTest
{
    private function getClient(array $options): Client
    {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            'http://example.com/',
        ));

        $jobObjectEncryptorProvider = new GenericObjectEncryptorProvider(
            new ObjectEncryptor(new EncryptorOptions(
                'stackId',
                'kmsKeyId',
                'kmsRegion',
                null,
                null
            )),
        );

        $existingJobFactory = new ExistingJobFactory(
            $storageClientFactory,
            $jobObjectEncryptorProvider,
        );

        return new Client(
            new NullLogger(),
            $existingJobFactory,
            'http://example.com/',
            'testToken',
            $options
        );
    }

    /**
     * @dataProvider dataProvider
     * @phpstan-param class-string<Throwable> $expectedException
     */
    public function testStateExceptions(string $stringCode, string $expectedException): void
    {
        // @todo client shouldn't retry on 400 errors?
        $mock = new MockHandler([
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                (string) json_encode([
                    'context' => ['stringCode' => $stringCode],
                ])
            ),
            new Response(
                400,
                ['Content-Type' => 'application/json'],
                (string) json_encode([
                    'context' => ['stringCode' => $stringCode],
                ])
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack, 'backoffMaxTries' => 1]);

        $this->expectException($expectedException);
        $client->postJobResult('123', 'success', new JobResult());
    }

    public function dataProvider(): Generator
    {
        yield [
            'stringCode' => StateTargetEqualsCurrentException::STRING_CODE,
            'expectedExceptions' => StateTargetEqualsCurrentException::class,
        ];
        yield [
            'stringCode' => StateTerminalException::STRING_CODE,
            'expectedExceptions' => StateTerminalException::class,
        ];
        yield [
            'stringCode' => StateTransitionForbiddenException::STRING_CODE,
            'expectedExceptions' => StateTransitionForbiddenException::class,
        ];
    }
}

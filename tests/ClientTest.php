<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
    private function getJobFactory(): JobFactory
    {
        $storageClientFactory = new JobFactory\StorageClientFactory('http://example.com/');
        $objectEncryptorFactory = new ObjectEncryptorFactory('alias/some-key', 'us-east-1', '', '');
        return new JobFactory($storageClientFactory, $objectEncryptorFactory);
    }

    private function getClient(array $options): Client
    {
        return new Client($this->getJobFactory(), 'http://example.com/', 'testToken', $options);
    }

    public function testGetJobData(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "123",
                    "project": {
                        "id": "456"
                    },
                    "token": {
                        "id": "789",
                        "token": "KBC::ProjectSecure::eJwBYAGf"
                    },
                    "params": {
                        "mode": "run",
                        "component": "keboola.test",
                        "config": "123456",
                        "configData": {
                            "parameters": {
                                "foo": "bar"
                            }
                        }
                    }
                }'
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        $job = $client->getJob('123');
        self::assertEquals('123', $job->getId());
        self::assertEquals('123456', $job->getConfigId());
        self::assertEquals('keboola.test', $job->getComponentId());
        self::assertEquals('456', $job->getProjectId());
        self::assertEquals('run', $job->getMode());
        self::assertEquals('created', $job->getStatus());
        self::assertNull($job->getResult());
        self::assertNull($job->getTag());
        self::assertNull($job->getRowId());
        self::assertFalse($job->isFinished());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getToken());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertCount(1, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertEquals('GET', $request->getMethod());
        self::assertEquals('testToken', $request->getHeader('X-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testSetJobResult(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{}'
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = new Client('http://example.com/', 'testToken', ['handler' => $stack]);
        $client->postJobResult('123', ['images' => ['digests' => ['keboola.test' => ['id' => '123']]]]);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('jobs/123', $request->getUri()->__toString());
        self::assertEquals('POST', $request->getMethod());
        self::assertEquals(
            '{"images":{"digests":{"keboola.test":{"id":"123"}}}}',
            $request->getBody()->getContents()
        );
        self::assertEquals('testToken', $request->getHeader('X-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }
}

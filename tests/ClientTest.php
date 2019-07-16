<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

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
        return new Client(new NullLogger(), $this->getJobFactory(), 'http://example.com/', 'testToken', $options);
    }

    public function testCreateClientInvalidBackoff(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage(
            'Invalid parameters when creating client: Value "abc" is invalid: This value should be a valid number'
        );
        new Client(
            new NullLogger(),
            $this->getJobFactory(),
            'http://example.com/',
            'testToken',
            ['backoffMaxTries' => 'abc']
        );
    }

    public function testCreateClientInvalidToken(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage(
            'Invalid parameters when creating client: Value "" is invalid: This value should not be blank.'
        );
        new Client(new NullLogger(), $this->getJobFactory(), 'http://example.com/', '');
    }

    public function testCreateClientInvalidUrl(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage(
            'Invalid parameters when creating client: Value "invalid url" is invalid: This value is not a valid URL.'
        );
        new Client(new NullLogger(), $this->getJobFactory(), 'invalid url', 'testToken');
    }

    public function testClientRequestResponse(): void
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
                        "token": "KBC::ProjectSecure::aSdF"
                    },
                    "status": "created",
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

    public function testInvalidResponse(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                'invalid json'
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Unable to parse response body into JSON: Syntax error');
        $client->getJob('123');
    }

    public function testLogger(): void
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
                    "status": "created",
                    "params": {
                        "mode": "run",
                        "component": "keboola.test",
                        "config": "123456"
                    }
                }'
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $logger = new TestLogger();
        $client = $this->getClient(['handler' => $stack, 'logger' => $logger, 'userAgent' => 'test agent']);
        $client->getJob('123');
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('test agent', $request->getHeader('User-Agent')[0]);
        self::assertTrue($logger->hasInfoThatContains('"GET  /1.1" 200 '));
        self::assertTrue($logger->hasInfoThatContains('test agent'));
    }

    public function testRetrySuccess(): void
    {
        $mock = new MockHandler([
            new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}'
            ),
            new Response(
                500,
                ['Content-Type' => 'application/json'],
                'Out of order'
            ),
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
                        "token": "KBC::ProjectSecure::aSdF"
                    },
                    "status": "created",
                    "params": {
                        "mode": "run",
                        "component": "keboola.test",
                        "config": "123456"
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
        self::assertCount(3, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        $request = $requestHistory[1]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        $request = $requestHistory[2]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
    }

    public function testRetryFailure(): void
    {
        $responses = [];
        for ($i = 0; $i < 30; $i++) {
            $responses[] = new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}'
            );
        }
        $mock = new MockHandler($responses);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        try {
            $client->getJob('123');
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertContains('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(11, $requestHistory);
    }

    public function testRetryFailureReducedBackoff(): void
    {
        $responses = [];
        for ($i = 0; $i < 30; $i++) {
            $responses[] = new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}'
            );
        }
        $mock = new MockHandler($responses);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack, 'backoffMaxTries' => 3]);
        try {
            $client->getJob('123');
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertContains('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(4, $requestHistory);
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
        $client = $this->getClient(['handler' => $stack]);
        $client->postJobResult(
            '123',
            JobFactory::STATUS_SUCCESS,
            ['images' => ['digests' => ['keboola.test' => ['id' => '123']]]]
        );
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertEquals('PUT', $request->getMethod());
        self::assertEquals(
            '{"status":"success","result":{"images":{"digests":{"keboola.test":{"id":"123"}}}}}',
            $request->getBody()->getContents()
        );
        self::assertEquals('testToken', $request->getHeader('X-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testCreateInvalidJob(): void
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
        $client = $this->getClient(['handler' => $stack]);
        $job = self::getMockBuilder(Job::class)
            ->disableOriginalConstructor()
            ->setMethods(['jsonSerialize'])
            ->getMock();
        $job->method('jsonSerialize')->willReturn(['foo' => fopen('php://memory', 'rw')]);
        /** @var Job $job */
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid job data: Type is not supported');
        $client->createJob($job);
    }
}

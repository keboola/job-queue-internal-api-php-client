<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\JobQueueInternalClient\Client;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
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
                        "token": "123-456-xxxxxxx"
                    },
                    "params": {
                        "mode": "run",
                        "component": "keboola.ex-db-snowflake",
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
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = new Client('http://example.com/', 'testToken', ['handler' => $stack]);
        $data = $client->getJobData('123');
        self::assertEquals('123', $data['id']);
        self::assertEquals('123456', $data['params']['config']);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('jobs/123', $request->getUri()->__toString());
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
        $client->postJobResult('123', ['images' => ['digests' => ['keboola.ex-db-snowflake' => ['id' => '123']]]]);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('jobs/123', $request->getUri()->__toString());
        self::assertEquals('POST', $request->getMethod());
        self::assertEquals(
            '{"images":{"digests":{"keboola.ex-db-snowflake":{"id":"123"}}}}',
            $request->getBody()->getContents()
        );
        self::assertEquals('testToken', $request->getHeader('X-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }
}

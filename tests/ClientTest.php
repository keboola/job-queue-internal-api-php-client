<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobResult;
use Keboola\JobQueueInternalClient\JobListOptions;
use Keboola\JobQueueInternalClient\JobPatchData;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;

class ClientTest extends BaseTest
{
    private function getJobFactory(): JobFactory
    {
        $storageClientFactory = new JobFactory\StorageClientFactory('http://example.com/');
        $objectEncryptorFactory = new ObjectEncryptorFactory('alias/some-key', 'us-east-1', '', '', '');
        return new JobFactory($storageClientFactory, $objectEncryptorFactory);
    }

    private function getClient(array $options, ?LoggerInterface $logger = null): Client
    {
        return new Client(
            $logger ?? new NullLogger(),
            $this->getJobFactory(),
            'http://example.com/',
            'testToken',
            $options
        );
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

    public function testCreateClientTooLowBackoff(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage(
            'Invalid parameters when creating client: Value "-1" is invalid: This value should be between 0 and 100.'
        );
        new Client(
            new NullLogger(),
            $this->getJobFactory(),
            'http://example.com/',
            'testToken',
            ['backoffMaxTries' => -1]
        );
    }

    public function testCreateClientTooHighBackoff(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage(
            'Invalid parameters when creating client: Value "101" is invalid: This value should be between 0 and 100.'
        );
        new Client(
            new NullLogger(),
            $this->getJobFactory(),
            'http://example.com/',
            'testToken',
            ['backoffMaxTries' => 101]
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

    public function testCreateClientMultipleErrors(): void
    {
        self::expectException(ClientException::class);
        self::expectExceptionMessage(
            'Invalid parameters when creating client: Value "invalid url" is invalid: This value is not a valid URL.'
            . "\n" . 'Value "" is invalid: This value should not be blank.' . "\n"
        );
        new Client(new NullLogger(), $this->getJobFactory(), 'invalid url', '');
    }

    public function testClientRequestResponse(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "123",
                    "runId": "123",
                    "projectId": "456",
                    "projectName": "Test project",
                    "tokenId": "789",
                    "#tokenString": "KBC::ProjectSecure::aSdF",
                    "tokenDescription": "my token",
                    "status": "created",
                    "desiredStatus": "processing",
                    "mode": "run",
                    "componentId": "keboola.test",
                    "configId": "123456",
                    "configData": {
                        "parameters": {
                            "foo": "bar"
                        }
                    },
                    "result": {},
                    "usageData": {},
                    "isFinished": false,
                    "branchId": "1234",
                    "variableValuesId": "1357",
                    "variableValuesData": {
                        "values": [{
                            "name": "boo",
                            "value": "bar"
                        }]
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
        self::assertEquals('Test project', $job->getProjectName());
        self::assertEquals('run', $job->getMode());
        self::assertEquals('created', $job->getStatus());
        self::assertEquals('1234', $job->getBranchId());
        self::assertEquals('1357', $job->getVariableValuesId());
        self::assertEquals(['values' => [['name' => 'boo', 'value' => 'bar']]], $job->getVariableValuesData());
        self::assertEquals([], $job->getResult());
        self::assertEquals([], $job->getUsageData());
        self::assertNull($job->getTag());
        self::assertIsArray($job->getConfigRowIds());
        self::assertEmpty($job->getConfigRowIds());
        self::assertFalse($job->isFinished());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getTokenString());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertCount(1, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertEquals('GET', $request->getMethod());
        self::assertEquals('testToken', $request->getHeader('X-JobQueue-InternalApi-Token')[0]);
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
                    "runId": "123",
                    "projectId": "456",
                    "projectName": "Test project",
                    "tokenId": "789",
                    "#tokenString": "KBC::ProjectSecure::aSdF",
                    "tokenDescription": "my token",
                    "status": "created",
                    "desiredStatus": "processing",
                    "mode": "run",
                    "componentId": "keboola.test",
                    "configId": "123456",
                    "configData": {
                        "parameters": {
                            "foo": "bar"
                        }
                    },
                    "result": {},
                    "usageData": {},
                    "isFinished": false,
                    "branchId": null
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
                    "runId": "123",
                    "projectId": "456",
                    "projectName": "Test project",
                    "tokenId": "789",
                    "#tokenString": "KBC::ProjectSecure::aSdF",
                    "tokenDescription": "my token",
                    "status": "created",
                    "desiredStatus": "processing",
                    "mode": "run",
                    "componentId": "keboola.test",
                    "configId": "123456",
                    "configData": {
                        "parameters": {
                            "foo": "bar"
                        }
                    },
                    "result": {},
                    "usageData": {},
                    "isFinished": false,
                    "branchId": null
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
        $client = $this->getClient(['handler' => $stack, 'backoffMaxTries' => 1]);
        try {
            $client->getJob('123');
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(2, $requestHistory);
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
            self::assertStringContainsString('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(4, $requestHistory);
    }

    public function testSetJobResult(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "123",
                    "runId": "123",
                    "projectId": "123",
                    "tokenId": "123",
                    "#tokenString": "KBC::XXX",
                    "componentId": "my-component",
                    "status": "processing",
                    "desiredStatus": "processing"
                }'
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        $result = $client->postJobResult(
            '123',
            JobFactory::STATUS_SUCCESS,
            (new JobResult())->setImages(['digests' => ['keboola.test' => ['id' => '123']]])
        );
        self::assertInstanceOf(Job::class, $result);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertEquals('PUT', $request->getMethod());
        self::assertEquals(
            '{"status":"success","result":{"message":null,"configVersion":null,' .
            '"images":{"digests":{"keboola.test":{"id":"123"}}}}}',
            $request->getBody()->getContents()
        );
        self::assertEquals('testToken', $request->getHeader('X-JobQueue-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testSetJobResultInvalid(): void
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
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid job ID: "".');
        $client->postJobResult(
            '',
            JobFactory::STATUS_SUCCESS,
            (new JobResult())->setImages(['digests' => ['keboola.test' => ['id' => '123']]])
        );
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
            ->onlyMethods(['jsonSerialize'])
            ->getMock();
        $job->method('jsonSerialize')->willReturn(['foo' => fopen('php://memory', 'rw')]);
        /** @var Job $job */
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Invalid job data: Type is not supported');
        $client->createJob($job);
    }

    public function testClientInvalidJobResponse(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '[{                    
                    "projectId": "456",
                    "projectName": "Test project",
                    "tokenId": "789",
                    "#tokenString": "KBC::ProjectSecure::aSdF",
                    "tokenDescription": "my token",
                    "status": "created"                    
                }]'
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $logger = new TestLogger();
        $client = $this->getClient(['handler' => $stack], $logger);
        $jobs = $client->getJobsWithIds(['123']);
        self::assertCount(0, $jobs);
        self::assertTrue($logger->hasErrorThatContains(
            'Failed to parse Job data: The child'
        ));
    }

    public function testClientGetJobWithEmptyIdThrowsException(): void
    {
        $client = $this->getClient([]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID: "".');
        $client->getJob('');
    }

    public function testClientGetJobsWithProjectIdDefaults(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '[{
                    "id": "123",
                    "runId": "123",
                    "projectId": "456",
                    "projectName": "Test project",
                    "tokenId": "789",
                    "#tokenString": "KBC::ProjectSecure::aSdF",
                    "tokenDescription": "my token",
                    "status": "created",
                    "desiredStatus": "processing",
                    "mode": "run",
                    "componentId": "keboola.test",
                    "configId": "123456",
                    "configData": {
                        "parameters": {
                            "foo": "bar"
                        }
                    },
                    "result": {},
                    "usageData": {},
                    "isFinished": false,
                    "branchId": null
                }]'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $logger = new TestLogger();
        $client = $this->getClient(['handler' => $stack], $logger);
        $jobs = $client->listJobs((new JobListOptions())->setProjects(['456']), true);

        self::assertCount(1, $jobs);
        /** @var Job $job */
        $job = $jobs[0];
        self::assertEquals('123', $job->getId());
        self::assertEquals('456', $job->getProjectId());
        self::assertEquals('', $job->getBranchId());

        $request = $mock->getLastRequest();
        self::assertEquals('projectId%5B%5D=456&limit=100', $request->getUri()->getQuery());
    }

    public function testClientGetJobsEscaping(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '[{
                    "id": "123",
                    "runId": "1234",
                    "projectId": "šěřč!@#%^$&",
                    "projectName": "Test project",
                    "tokenId": "789",
                    "#tokenString": "KBC::ProjectSecure::aSdF",
                    "tokenDescription": "my token",
                    "status": "created",
                    "desiredStatus": "processing",
                    "mode": "run",
                    "componentId": "th!$ |& n°t valid",
                    "configId": "123456",
                    "configData": {
                        "parameters": {
                            "foo": "bar"
                        }
                    },
                    "result": {},
                    "usageData": {},
                    "isFinished": false,
                    "branchId": null
                }]'
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $logger = new TestLogger();
        $client = $this->getClient(['handler' => $stack], $logger);
        $jobs = $client->listJobs(
            (new JobListOptions())->setProjects(['šěřč!@#%^$&'])->setComponents(['th!$ |& n°t valid']),
            true
        );

        self::assertCount(1, $jobs);
        /** @var Job $job */
        $job = $jobs[0];
        self::assertEquals('123', $job->getId());
        self::assertEquals('šěřč!@#%^$&', $job->getProjectId());

        $request = $mock->getLastRequest();
        self::assertEquals(
            'componentId%5B%5D=th%21%24+%7C%26+n%C2%B0t+valid&' .
                'projectId%5B%5D=%C5%A1%C4%9B%C5%99%C4%8D%21%40%23%25%5E%24%26&limit=100',
            $request->getUri()->getQuery()
        );
    }

    public function testClientGetJobsWithIds(): void
    {
        $count = 1001;
        $jobData = [
            'id'=> '123',
            'runId' => '123',
            'projectId'=> '456',
            'projectName'=> 'Test project',
            'tokenId'=> '789',
            '#tokenString'=> 'KBC=>=>ProjectSecure=>=>aSdF',
            'tokenDescription'=> 'my token',
            'status'=> 'created',
            'desiredStatus'=> 'processing',
            'mode'=> 'run',
            'componentId'=> 'keboola.test',
            'configId'=> '123456',
            'configData'=> [
                'parameters'=> [
                    'foo'=> 'bar',
                ],
            ],
            'result'=> [],
            'usageData'=> [],
            'isFinished'=> false,
            'branchId' => '1234',
        ];
        $queue = array_fill(0, 10, function () use ($jobData): Response {
            return new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode(array_fill(
                    0,
                    100,
                    $jobData
                ))
            );
        });
        $queue[] = new Response(
            200,
            ['Content-Type' => 'application/json'],
            (string) json_encode([$jobData])
        );
        $mock = new MockHandler($queue);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $logger = new TestLogger();
        $client = $this->getClient(['handler' => $stack], $logger);

        $startId = 1000000;
        $endId = $startId + $count - 1;
        $jobs = $client->getJobsWithIds(range($startId, $endId, 1));

        self::assertCount($count, $jobs);
        /** @var Job $job */
        $job = $jobs[0];
        self::assertEquals('123', $job->getId());
        self::assertEquals('456', $job->getProjectId());

        self::assertEquals(0, $mock->count());
        $params = [];
        foreach ($requestHistory as $request) {
            $requestUri = $request['request']->getUri()->getQuery();
            if (preg_match('#offset=([0-9]+)#', $requestUri, $matches)) {
                $offset = $matches[1];
            } else {
                $offset = '';
            }
            preg_match('#limit=([0-9]+)#', $requestUri, $matches);
            $params[] = ['offset' => $offset, 'limit' => $matches[1]];
        }
        self::assertEquals(
            [
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
                ['offset' => '', 'limit' => '100'],
            ],
            $params
        );
        $request = $mock->getLastRequest();
        self::assertStringStartsWith('id%5B%5D=1001000', $request->getUri()->getQuery());
        self::assertLessThan(2000, strlen($request->getUri()->getQuery()));
    }


    public function testClientGetJobsPaging(): void
    {
        $jobData = [
            'id'=> '123',
            'runId' => '123',
            'projectId'=> '456',
            'projectName'=> 'Test project',
            'tokenId'=> '789',
            '#tokenString'=> 'KBC=>=>ProjectSecure=>=>aSdF',
            'tokenDescription'=> 'my token',
            'status'=> 'created',
            'desiredStatus'=> 'processing',
            'mode'=> 'run',
            'componentId'=> 'keboola.test',
            'configId'=> '123456',
            'configData'=> [
                'parameters'=> [
                    'foo'=> 'bar',
                ],
            ],
            'result'=> [],
            'usageData'=> [],
            'isFinished'=> false,
            'branchId' => null,
        ];
        $queue = array_fill(0, 10, function () use ($jobData): Response {
            return new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode(array_fill(
                    0,
                    100,
                    $jobData
                ))
            );
        });
        $queue[] = new Response(
            200,
            ['Content-Type' => 'application/json'],
            (string) json_encode([$jobData])
        );
        $mock = new MockHandler($queue);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $logger = new TestLogger();
        $client = $this->getClient(['handler' => $stack], $logger);
        $jobs = $client->listJobs((new JobListOptions()), true);
        self::assertCount(1001, $jobs);
        $request = $mock->getLastRequest();
        self::assertEquals('offset=1000&limit=100', $request->getUri()->getQuery());
        self::assertEquals(0, $mock->count());
    }

    public function testPatchJobStatus(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "123",
                    "runId": "123",
                    "projectId": "123",
                    "tokenId": "123",
                    "#tokenString": "KBC::XXX",
                    "componentId": "my-component",
                    "status": "processing",
                    "desiredStatus": "processing"    
                }'
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        $result = $client->patchJob(
            '123',
            (new JobPatchData())->setStatus(JobFactory::STATUS_PROCESSING)
        );
        self::assertInstanceOf(Job::class, $result);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertEquals('PUT', $request->getMethod());
        self::assertEquals(
            '{"status":"processing"}',
            $request->getBody()->getContents()
        );
        self::assertEquals('testToken', $request->getHeader('X-JobQueue-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testPatchJobDesiredStatus(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "id": "123",
                    "runId": "123",
                    "projectId": "123",
                    "tokenId": "123",
                    "#tokenString": "KBC::XXX",
                    "componentId": "my-component",
                    "status": "processing",
                    "desiredStatus": "processing"   
                }'
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->getClient(['handler' => $stack]);
        $result = $client->patchJob(
            '123',
            (new JobPatchData())->setDesiredStatus(JobFactory::DESIRED_STATUS_TERMINATING)
        );
        self::assertInstanceOf(Job::class, $result);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertEquals('PUT', $request->getMethod());
        self::assertEquals(
            '{"desiredStatus":"terminating"}',
            $request->getBody()->getContents()
        );
        self::assertEquals('testToken', $request->getHeader('X-JobQueue-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testPatchJobInvalidJobId(): void
    {
        $client = $this->getClient([]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID: "".');
        $client->patchJob('', new JobPatchData());
    }

    public function testClientUpdateJobWithEmptyIdThrowsException(): void
    {
        $objectEncryptorFactory = new ObjectEncryptorFactory('alias/some-key', 'us-east-1', '', '', '');
        $job = new Job($objectEncryptorFactory, [
            'status' => JobFactory::STATUS_SUCCESS,
            'projectId' => 'test',
            'id' => '',
        ]);
        $client = $this->getClient([]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID: "".');
        $client->updateJob($job);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use DateTimeImmutable;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Response;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\ExistingJobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\JobListOptions;
use Keboola\JobQueueInternalClient\JobPatchData;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\JobQueueInternalClient\Result\JobResult;
use Keboola\ObjectEncryptor\EncryptorOptions;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Psr\Http\Message\RequestInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use stdClass;

class ClientTest extends BaseTest
{
    private function createClient(
        ?string $internalAuthToken = null,
        ?string $storageApiToken = null,
        ?string $applicationToken = null,
        array $options = [],
        ?LoggerInterface $logger = null,
    ): Client {
        $storageClientFactory = new StorageClientPlainFactory(new ClientOptions(
            'http://example.com/',
        ));

        $objectEncryptor = new ObjectEncryptor(new EncryptorOptions(
            'stackId',
            'kmsKeyId',
            'kmsRegion',
            null,
            null,
        ));

        $jobFactory = new ExistingJobFactory(
            $storageClientFactory,
            new JobObjectEncryptor($objectEncryptor),
        );

        return new Client(
            $logger ?? new NullLogger(),
            $jobFactory,
            'http://example.com/',
            $internalAuthToken,
            $storageApiToken,
            $applicationToken,
            $options,
        );
    }

    private function createClientWithInternalToken(
        array $options = [],
        ?LoggerInterface $logger = null,
    ): Client {
        return $this->createClient(
            internalAuthToken: 'testToken',
            options: $options,
            logger: $logger,
        );
    }

    public function testCreateClientInvalidBackoff(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "abc" is invalid: This value should be a valid number',
        );
        new Client(
            new NullLogger(),
            $this->createMock(ExistingJobFactory::class),
            'http://example.com/',
            'testToken',
            null,
            null,
            ['backoffMaxTries' => 'abc'],
        );
    }

    public function testCreateClientTooLowBackoff(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "-1" is invalid: This value should be between 0 and 100.',
        );
        new Client(
            new NullLogger(),
            $this->createMock(ExistingJobFactory::class),
            'http://example.com/',
            'testToken',
            null,
            null,
            ['backoffMaxTries' => -1],
        );
    }

    public function testCreateClientTooHighBackoff(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "101" is invalid: This value should be between 0 and 100.',
        );
        new Client(
            new NullLogger(),
            $this->createMock(ExistingJobFactory::class),
            'http://example.com/',
            'testToken',
            null,
            null,
            ['backoffMaxTries' => 101],
        );
    }

    public static function provideInvalidTokensConfiguration(): iterable
    {
        yield 'no token' => [
            'storageApiToken' => null,
            'internalAuthToken' => null,
            'applicationToken' => null,
            'expectedError' => 'No token provided (internalQueueToken, storageApiToken and applicationToken are empty)',
        ];

        yield 'storage + internal token' => [
            'storageApiToken' => 'storageToken',
            'internalAuthToken' => 'internalToken',
            'applicationToken' => null,
            'expectedError' => 'More than one authentication token provided',
        ];

        yield 'storage + application token' => [
            'storageApiToken' => 'storageToken',
            'internalAuthToken' => null,
            'applicationToken' => 'applicationToken',
            'expectedError' => 'More than one authentication token provided',
        ];

        yield 'internal + application token' => [
            'storageApiToken' => null,
            'internalAuthToken' => 'internalToken',
            'applicationToken' => 'applicationToken',
            'expectedError' => 'More than one authentication token provided',
        ];

        yield 'empty storage token' => [
            'storageApiToken' => '',
            'internalAuthToken' => null,
            'applicationToken' => null,
            'expectedError' => 'Value "" is invalid: This value should not be blank.',
        ];

        yield 'empty internal token' => [
            'storageApiToken' => null,
            'internalAuthToken' => '',
            'applicationToken' => null,
            'expectedError' => 'Value "" is invalid: This value should not be blank.',
        ];

        yield 'empty application token' => [
            'storageApiToken' => null,
            'internalAuthToken' => null,
            'applicationToken' => '',
            'expectedError' => 'Value "" is invalid: This value should not be blank.',
        ];
    }

    /** @dataProvider provideInvalidTokensConfiguration */
    public function testCreateClientInvalidTokens(
        ?string $storageApiToken,
        ?string $internalAuthToken,
        ?string $applicationToken,
        string $expectedError,
    ): void {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage($expectedError);
        new Client(
            new NullLogger(),
            $this->createMock(ExistingJobFactory::class),
            'http://example.com/',
            $storageApiToken,
            $internalAuthToken,
            $applicationToken,
        );
    }

    public function testCreateClientInvalidUrl(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "invalid url" is invalid: This value is not a valid URL.',
        );
        new Client(
            new NullLogger(),
            $this->createMock(ExistingJobFactory::class),
            'invalid url',
            'testToken',
            null,
            null,
        );
    }

    public function testCreateClientMultipleErrors(): void
    {
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'Invalid parameters when creating client: Value "invalid url" is invalid: This value is not a valid URL.'
            . "\n" . 'Value "" is invalid: This value should not be blank.' . "\n",
        );
        new Client(
            new NullLogger(),
            $this->createMock(ExistingJobFactory::class),
            'invalid url',
            '',
            null,
            null,
        );
    }

    public function provideRequestResponseTestData(): iterable
    {
        yield 'storage token auth' => [
            'storageApiToken' => 'testToken',
            'internalAuthToken' => null,
            'applicationToken' => null,
            'expectedAuthHeader' => 'X-StorageApi-Token',
        ];

        yield 'internal token auth' => [
            'storageApiToken' => null,
            'internalAuthToken' => 'testToken',
            'applicationToken' => null,
            'expectedAuthHeader' => 'X-JobQueue-InternalApi-Token',
        ];

        yield 'application token auth' => [
            'storageApiToken' => null,
            'internalAuthToken' => null,
            'applicationToken' => 'testToken',
            'expectedAuthHeader' => 'X-KBC-ManageApiToken',
        ];
    }

    /** @dataProvider provideRequestResponseTestData */
    public function testClientRequestResponse(
        ?string $storageApiToken,
        ?string $internalAuthToken,
        ?string $applicationToken,
        string $expectedAuthHeader,
    ): void {
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
                    "branchType": "dev",
                    "variableValuesId": "1357",
                    "variableValuesData": {
                        "values": [{
                            "name": "boo",
                            "value": "bar"
                        }]
                    }
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = $this->createClient(
            internalAuthToken: $internalAuthToken,
            storageApiToken: $storageApiToken,
            applicationToken: $applicationToken,
            options: ['handler' => $stack],
        );
        $job = $client->getJob('123');

        self::assertSame('123', $job->getId());
        self::assertSame('123456', $job->getConfigId());
        self::assertSame('keboola.test', $job->getComponentId());
        self::assertSame('456', $job->getProjectId());
        self::assertSame('Test project', $job->getProjectName());
        self::assertSame('run', $job->getMode());
        self::assertSame('created', $job->getStatus());
        self::assertSame('1234', $job->getBranchId());
        self::assertSame('1357', $job->getVariableValuesId());
        self::assertSame(['values' => [['name' => 'boo', 'value' => 'bar']]], $job->getVariableValuesData());
        self::assertSame([], $job->getResult());
        self::assertSame([], $job->getUsageData());
        self::assertNull($job->getTag());
        self::assertIsArray($job->getConfigRowIds());
        self::assertEmpty($job->getConfigRowIds());
        self::assertFalse($job->isFinished());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getTokenString());
        self::assertSame(['parameters' => ['foo' => 'bar']], $job->getConfigData());

        self::assertCount(1, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertSame('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertSame('GET', $request->getMethod());
        self::assertSame('testToken', $request->getHeaderLine($expectedAuthHeader));
        self::assertSame('Internal PHP Client', $request->getHeaderLine('User-Agent'));
        self::assertSame('application/json', $request->getHeaderLine('Content-type'));
    }

    public function testInvalidResponse(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                'invalid json',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Unable to parse response body into JSON: Syntax error');
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
                    "branchId": null,
                    "branchType": "default"
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $client = $this->createClientWithInternalToken(
            options: [
                'handler' => $stack,
                'logger' => $logger,
                'userAgent' => 'test agent',
            ],
        );
        $client->getJob('123');
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('test agent', $request->getHeader('User-Agent')[0]);
        self::assertTrue($logsHandler->hasInfoThatContains('"GET  /1.1" 200 '));
        self::assertTrue($logsHandler->hasInfoThatContains('test agent'));
    }

    public function testRetrySuccess(): void
    {
        $mock = new MockHandler([
            new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}',
            ),
            new Response(
                500,
                ['Content-Type' => 'application/json'],
                'Out of order',
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
                    "branchId": null,
                    "branchType": "default"
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
            logger: $logger,
        );
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

        //phpcs:disable Generic.Files.LineLength.MaxExceeded
        self::assertTrue($logsHandler->hasNoticeThatContains('Got a 500 error with this message: Server error: `GET http://example.com/jobs/123` resulted in a `500 Internal Server Error` response:
{"message" => "Out of order"}
, retrying.'));
        self::assertTrue($logsHandler->hasNoticeThatContains('Got a 500 error with this message: Server error: `GET http://example.com/jobs/123` resulted in a `500 Internal Server Error` response:
Out of order
, retrying.'));
        //phpcs:enable Generic.Files.LineLength.MaxExceeded
    }

    public function testRetryFailure(): void
    {
        $responses = [];
        for ($i = 0; $i < 30; $i++) {
            $responses[] = new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}',
            );
        }
        $mock = new MockHandler($responses);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $client = $this->createClientWithInternalToken(
            options: [
                'handler' => $stack,
                'backoffMaxTries' => 1,
            ],
            logger: $logger,
        );
        try {
            $client->getJob('123');
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('500 Internal Server Error', $e->getMessage());
        }
        self::assertCount(2, $requestHistory);

        //phpcs:disable Generic.Files.LineLength.MaxExceeded
        self::assertTrue($logsHandler->hasNoticeThatContains('Got a 500 error with this message: Server error: `GET http://example.com/jobs/123` resulted in a `500 Internal Server Error` response:
{"message" => "Out of order"}
, retrying.'));
        self::assertTrue($logsHandler->hasNoticeThatMatches('#We have tried this 1 times.\s*Giving up.#'));
        //phpcs:enable Generic.Files.LineLength.MaxExceeded
    }

    public function testRetryFailureReducedBackoff(): void
    {
        $responses = [];
        for ($i = 0; $i < 30; $i++) {
            $responses[] = new Response(
                500,
                ['Content-Type' => 'application/json'],
                '{"message" => "Out of order"}',
            );
        }
        $mock = new MockHandler($responses);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: [
                'handler' => $stack,
                'backoffMaxTries' => 3,
            ],
        );
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
                    "desiredStatus": "processing",
                    "branchType": "default"
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );
        $result = $client->postJobResult(
            '123',
            JobInterface::STATUS_SUCCESS,
            (new JobResult())->setImages(['digests' => ['keboola.test' => ['id' => '123']]]),
            (new JobMetrics())
                ->setInputTablesBytesSum(112233445566)
                ->setOutputTablesBytesSum(112233445577)
                ->setBackendSize('small')
                ->setBackendContext('wlm'),
        );
        self::assertInstanceOf(Job::class, $result);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertSame('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertSame('PUT', $request->getMethod());
        self::assertSame(
            [
                'status' => 'success',
                'result' => [
                    'message' => null,
                    'configVersion' => null,
                    'images' => [
                        'digests' => [
                            'keboola.test' => [
                                'id' => '123',
                            ],
                        ],
                    ],
                    'input' => [
                        'tables' => [],
                    ],
                    'output' => [
                        'tables' => [],
                    ],
                ],
                'metrics' => [
                    'storage' => [
                        'inputTablesBytesSum' => 112233445566,
                        'outputTablesBytesSum' => 112233445577,
                    ],
                    'backend' => [
                        'size' => 'small',
                        'containerSize' => null,
                        'context' => 'wlm',
                    ],
                ],
            ],
            json_decode($request->getBody()->getContents(), true),
        );
        self::assertSame('testToken', $request->getHeader('X-JobQueue-InternalApi-Token')[0]);
        self::assertSame('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertSame('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testSetJobResultInvalid(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{}',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID: "".');
        $client->postJobResult(
            '',
            JobInterface::STATUS_SUCCESS,
            (new JobResult())->setImages(['digests' => ['keboola.test' => ['id' => '123']]]),
        );
    }

    public function testCreateInvalidJob(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{}',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );
        $job = $this->getMockBuilder(Job::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['jsonSerialize'])
            ->getMock();
        $job->method('jsonSerialize')->willReturn(['foo' => fopen('php://memory', 'rw')]);
        /** @var Job $job */
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job data: Type is not supported');
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
                }]',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
            logger: $logger,
        );
        $jobs = $client->getJobsWithIds(['123']);
        self::assertCount(0, $jobs);
        self::assertTrue($logsHandler->hasErrorThatContains(
            'Failed to parse Job data: The child',
        ));
    }

    public function testClientGetJobWithEmptyIdThrowsException(): void
    {
        $client = $this->createClientWithInternalToken();

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
                    "branchId": null,
                    "branchType": "default"
                }]',
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
            logger: $logger,
        );
        $jobs = $client->listJobs((new JobListOptions())->setProjects(['456']), true);

        self::assertCount(1, $jobs);
        /** @var Job $job */
        $job = $jobs[0];
        self::assertEquals('123', $job->getId());
        self::assertEquals('456', $job->getProjectId());
        self::assertEquals('', $job->getBranchId());

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        /** @var RequestInterface $request */
        $query = $request->getUri()->getQuery();
        self::assertStringStartsWith('projectId%5B%5D=456&limit=100', $query);
        self::assertStringContainsString('delayedStartTimeTo=', $query);
    }

    /** @dataProvider provideListJobsOptionsTestData */
    public function testListJobsOptions(JobListOptions $jobListOptions, string $expectedRequestUri): void
    {
        $mock = new MockHandler([
            new Response(200, ['Content-Type' => 'application/json'], (string) json_encode([
                [
                    'id' => '123',
                    'runId' => '123',
                    'projectId' => '456',
                    'projectName' => 'Test project',
                    'tokenId' => '789',
                    '#tokenString' => 'KBC::ProjectSecure::aSdF',
                    'tokenDescription' => 'my token',
                    'status' => 'created',
                    'desiredStatus' => 'processing',
                    'mode' => 'run',
                    'componentId' => 'keboola.test',
                    'configId' => '123456',
                    'configData' => [
                        'parameters' => [
                            'foo' => 'bar',
                        ],
                    ],
                    'result' => new stdClass(),
                    'usageData' => new stdClass(),
                    'isFinished' => false,
                    'branchId' => null,
                ],
            ])),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );
        $client->listJobs($jobListOptions, true);

        $request = $requestHistory[0]['request'];
        $query = $request->getUri()->getQuery();
        self::assertStringStartsWith((string) parse_url($expectedRequestUri, PHP_URL_QUERY), $query);
        self::assertStringContainsString('delayedStartTimeTo=', $query);
        self::assertStringContainsString('delayedStartTimeToIncludeNull=true', $query);
    }

    public function provideListJobsOptionsTestData(): iterable
    {
        yield 'empty options' => [
            'options' => new JobListOptions(),
            'url' => 'http://example.com/jobs?limit=100',
        ];

        yield 'sort by id, asc' => [
            'options' => (new JobListOptions())
                ->setSortBy('id')
                ->setSortOrder('asc'),
            'url' => 'http://example.com/jobs?limit=100&sortBy=id&sortOrder=asc',
        ];

        yield 'filter date range' => [
            'options' => (new JobListOptions())
                ->setCreatedTimeFrom(new DateTimeImmutable('2022-03-01T12:17:05+10:00'))
                ->setCreatedTimeTo(new DateTimeImmutable('2022-07-14T05:11:45-08:20')),
            // phpcs:ignore
            'url' => 'http://example.com/jobs?limit=100&createdTimeFrom=2022-03-01T12%3A17%3A05%2B10%3A00&createdTimeTo=2022-07-14T05%3A11%3A45-08%3A20',
        ];

        yield 'filter by delayedStartTime' => [
            'options' => (new JobListOptions()),
            // phpcs:ignore
            'url' => 'http://example.com/jobs?limit=100&delayedStartTimeTo=',
        ];
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
                    "branchId": null,
                    "branchType": "default"
                }]',
            ),
        ]);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
            logger: $logger,
        );
        $jobs = $client->listJobs(
            (new JobListOptions())->setProjects(['šěřč!@#%^$&'])->setComponents(['th!$ |& n°t valid']),
            true,
        );

        self::assertCount(1, $jobs);
        /** @var Job $job */
        $job = $jobs[0];
        self::assertEquals('123', $job->getId());
        self::assertEquals('šěřč!@#%^$&', $job->getProjectId());

        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        /** @var RequestInterface $request */
        $query = $request->getUri()->getQuery();
        self::assertStringStartsWith(
            'componentId%5B%5D=th%21%24+%7C%26+n%C2%B0t+valid&' .
            'projectId%5B%5D=%C5%A1%C4%9B%C5%99%C4%8D%21%40%23%25%5E%24%26&limit=100',
            $query,
        );
        self::assertStringContainsString('delayedStartTimeTo=', $query);
    }

    public function testClientGetJobsWithIds(): void
    {
        $count = 1001;
        $jobData = [
            'id' => '123',
            'runId' => '123',
            'projectId' => '456',
            'projectName' => 'Test project',
            'tokenId' => '789',
            '#tokenString' => 'KBC=>=>ProjectSecure=>=>aSdF',
            'tokenDescription' => 'my token',
            'status' => 'created',
            'desiredStatus' => 'processing',
            'mode' => 'run',
            'componentId' => 'keboola.test',
            'configId' => '123456',
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'result' => [],
            'usageData' => [],
            'isFinished' => false,
            'branchId' => '1234',
            'branchType' => BranchType::DEV->value,
        ];
        $queue = array_fill(0, 10, function () use ($jobData): Response {
            return new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode(array_fill(
                    0,
                    100,
                    $jobData,
                )),
            );
        });
        $queue[] = new Response(
            200,
            ['Content-Type' => 'application/json'],
            (string) json_encode([$jobData]),
        );
        $mock = new MockHandler($queue);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
            logger: $logger,
        );

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
            $params[] = ['offset' => $offset, 'limit' => $matches[1] ?? ''];
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
            $params,
        );
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        /** @var RequestInterface $request */
        $query = $request->getUri()->getQuery();
        self::assertIsString($query);
        self::assertStringStartsWith('id%5B%5D=1001000', $query);
        self::assertLessThan(2000, strlen($query));
    }


    public function testClientGetJobsPaging(): void
    {
        $jobData = [
            'id' => '123',
            'runId' => '123',
            'projectId' => '456',
            'projectName' => 'Test project',
            'tokenId' => '789',
            '#tokenString' => 'KBC=>=>ProjectSecure=>=>aSdF',
            'tokenDescription' => 'my token',
            'status' => 'created',
            'desiredStatus' => 'processing',
            'mode' => 'run',
            'componentId' => 'keboola.test',
            'configId' => '123456',
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'result' => [],
            'usageData' => [],
            'isFinished' => false,
            'branchId' => null,
            'branchType' => BranchType::DEFAULT->value,
        ];
        $queue = array_fill(0, 10, function () use ($jobData): Response {
            return new Response(
                200,
                ['Content-Type' => 'application/json'],
                (string) json_encode(array_fill(
                    0,
                    100,
                    $jobData,
                )),
            );
        });
        $queue[] = new Response(
            200,
            ['Content-Type' => 'application/json'],
            (string) json_encode([$jobData]),
        );
        $mock = new MockHandler($queue);

        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);

        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
            logger: $logger,
        );
        $jobs = $client->listJobs((new JobListOptions()), true);
        self::assertCount(1001, $jobs);
        $request = $mock->getLastRequest();
        self::assertNotNull($request);
        /** @var RequestInterface $request */
        $query = $request->getUri()->getQuery();
        self::assertStringStartsWith('offset=1000&limit=100', $query);
        self::assertStringContainsString('delayedStartTimeTo=', $query);
        self::assertStringContainsString('delayedStartTimeToIncludeNull=true', $query);
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
                    "desiredStatus": "processing",
                    "branchType": "default"
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );
        $result = $client->patchJob(
            '123',
            (new JobPatchData())->setStatus(JobInterface::STATUS_PROCESSING),
        );
        self::assertInstanceOf(Job::class, $result);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertEquals('PUT', $request->getMethod());
        self::assertEquals(
            '{"status":"processing"}',
            $request->getBody()->getContents(),
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
                    "desiredStatus": "processing",
                    "branchType": "default"
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );
        $result = $client->patchJob(
            '123',
            (new JobPatchData())->setDesiredStatus(JobInterface::DESIRED_STATUS_TERMINATING),
        );
        self::assertInstanceOf(Job::class, $result);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('http://example.com/jobs/123', $request->getUri()->__toString());
        self::assertEquals('PUT', $request->getMethod());
        self::assertEquals(
            '{"desiredStatus":"terminating"}',
            $request->getBody()->getContents(),
        );
        self::assertEquals('testToken', $request->getHeader('X-JobQueue-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testPatchJobInvalidJobId(): void
    {
        $client = $this->createClientWithInternalToken();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID: "".');
        $client->patchJob('', new JobPatchData());
    }

    public function testClientGetJobsDurationSumWithEmptyIdThrowsException(): void
    {
        $client = $this->createClientWithInternalToken();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid project ID: "".');
        $client->getJobsDurationSum('');
    }

    public function testClientGetJobsDurationSum(): void
    {
        $mock = new MockHandler([
            new Response(
                200,
                ['Content-Type' => 'application/json'],
                '{
                    "stats": {
                        "durationSum": 456
                    }
                }',
            ),
        ]);
        // Add the history middleware to the handler stack.
        $requestHistory = [];
        $history = Middleware::history($requestHistory);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );
        $durationSum = $client->getJobsDurationSum('123');
        self::assertSame(456, $durationSum);

        self::assertCount(1, $requestHistory);
        /** @var Request $request */
        $request = $requestHistory[0]['request'];
        self::assertEquals('http://example.com/stats/projects/123', $request->getUri()->__toString());
        self::assertEquals('GET', $request->getMethod());
        self::assertEquals('testToken', $request->getHeader('X-JobQueue-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testPatchJobResult(): void
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
                    "desiredStatus": "processing",
                    "branchType": "default"
                }',
            ),
        ]);

        $container = [];
        $history = Middleware::history($container);
        $stack = HandlerStack::create($mock);
        $stack->push($history);
        $client = $this->createClientWithInternalToken(
            options: ['handler' => $stack],
        );

        $patchData = [
            'message' => 'xyz',
            'configVersion' => '2',
        ];
        $result = $client->patchJobResult(
            '123',
            $patchData,
        );

        self::assertInstanceOf(Job::class, $result);
        self::assertCount(1, $container);
        /** @var Request $request */
        $request = $container[0]['request'];
        self::assertEquals('http://example.com/jobs/123/result', $request->getUri()->__toString());
        self::assertEquals('PATCH', $request->getMethod());
        self::assertEquals(
            json_encode($patchData),
            $request->getBody()->getContents(),
        );
        self::assertEquals('testToken', $request->getHeader('X-JobQueue-InternalApi-Token')[0]);
        self::assertEquals('Internal PHP Client', $request->getHeader('User-Agent')[0]);
        self::assertEquals('application/json', $request->getHeader('Content-type')[0]);
    }

    public function testPatchJobResultInvalidJobId(): void
    {
        $client = $this->createClientWithInternalToken();

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Invalid job ID: "".');

        $client->patchJobResult('', []);
    }
}

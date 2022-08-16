<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class FullJobDefinitionTest extends BaseTest
{
    public function testValidJobMaximal(): void
    {
        $expectedData = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'tokenId' => '12345',
            'tokenDescription' => '?',
            'projectId' => '123',
            'configId' => '123',
            'configData' => [
                'foo' => 'bar',
            ],
            'componentId' => 'keboola.test',
            'mode' => 'run',
            'id' => '1234',
            'runId' => '1234',
            'result' => [
                'bar' => 'foo',
            ],
            'status' => JobInterface::STATUS_CREATED,
            'desiredStatus' => JobInterface::DESIRED_STATUS_PROCESSING,
            'parallelism' => null,
            'type' => 'standard',
        ];
        $definition = new FullJobDefinition();
        $processedData = $definition->processData($expectedData);
        $expectedData['configRowIds'] = [];
        $expectedData['tag'] = null;
        $expectedData['isFinished'] = false;
        $expectedData['orchestrationJobId'] = null;
        self::assertEquals($expectedData, $processedData);
    }

    public function testValidJobFull(): void
    {
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'tokenId' => '12345',
            'projectId' => '123',
            'configId' => '123',
            'componentId' => 'keboola.test',
            'mode' => 'forceRun',
            'configRowIds' => ['234'],
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'tag' => 'latest',
            'id' => '1234',
            'runId' => '1234',
            'status' => JobInterface::STATUS_CREATED,
            'desiredStatus' => JobInterface::DESIRED_STATUS_PROCESSING,
            'parallelism' => null,
            'type' => 'standard',
            'extraKey' => 'ignored',
            'backend' => [
                'type' => 'large',
                'containerType' => 'small',
            ],
            'metrics' => [
                'storage' => [
                    'inputTablesBytesSum' => 123,
                    'outputTablesBytesSum' => 456,
                    'storageExtraKey' => 'ignored',
                ],
                'backend' => [
                    'size' => 'medium',
                    'containerSize' => 'large',
                    'backendExtraKey' => 'ignored',
                ],
            ],
            'orchestrationJobId' => '123456789',
        ];
        unset($data['extraKey']);
        unset($data['metrics']['storage']['storageExtraKey']);
        unset($data['metrics']['backend']['backendExtraKey']);
        $definition = new FullJobDefinition();
        self::assertEquals(array_merge($data, [
            'isFinished' => false,
        ]), $definition->processData($data));
    }

    public function invalidJobProvider(): array
    {
        return [
            'Missing token' => [
                [
                    'id' => '12345',
                    'runId' => '12345',
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#The child (node|config) "\#tokenString" (at path|under) "job" must be configured.#',
            ],
            'Invalid mode' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'invalid',
                ],
                '#Invalid configuration for path "job.mode": Mode must be one of "run", "forceRun" ' .
                'or "debug" \(or "dry-run","prepare","input","full","single"\).#',
            ],
            'Invalid configData' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'configData' => '345',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#Invalid type for path "job.configData". Expected "?array"?, but got "?string"?#',
            ],
            'Invalid configRowIds' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'configRowIds' => '123',
                ],
                '#Invalid type for path "job.configRowIds". Expected "?array"?, but got "?string"?#',
            ],
            'Invalid tag' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'tag' => ['234'],
                ],
                '#Invalid type for path "job.tag". Expected "?scalar"?, but got "?array"?.#',
            ],
            'Missing id' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#The child (node|config) "id" (at path|under) "job" must be configured.#',
            ],
            'Missing status' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#The child (node|config) "status" (at path|under) "job" must be configured.#',
            ],
            'Invalid status' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'invalid',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#Invalid configuration for path "job.status": Status must be one of cancelled, created, error, ' .
                    'processing, success, terminated, terminating, waiting, warning.#',
            ],
            'Missing project id' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#The child (node|config) "projectId" (at path|under) "job" must be configured.#',
            ],
            'Missing token id' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#The child (node|config) "tokenId" (at path|under) "job" must be configured.#',
            ],
            'Invalid metrics' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'metrics' => 'test',
                ],
                '#Invalid type for path "job.metrics". Expected "array", but got "string"#',
            ],
            'Invalid storage metrics' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'metrics' => [
                        'storage' => 'test',
                    ],
                ],
                '#Invalid type for path "job.metrics.storage". Expected "array", but got "string"#',
            ],
            'Invalid storage inputTablesBytesSum metrics' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'metrics' => [
                        'storage' => [
                            'inputTablesBytesSum' => [],
                        ],
                    ],
                ],
                '#Invalid type for path "job.metrics.storage.inputTablesBytesSum". Expected "scalar",' .
                    ' but got "array".#',
            ],
            'Invalid storage outputTablesBytesSum metrics' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'metrics' => [
                        'storage' => [
                            'outputTablesBytesSum' => [],
                        ],
                    ],
                ],
                '#Invalid type for path "job.metrics.storage.outputTablesBytesSum". Expected "scalar",' .
                    ' but got "array".#',
            ],
            'Invalid backend metrics' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'metrics' => [
                        'backend' => 'test',
                    ],
                ],
                '#Invalid type for path "job.metrics.backend". Expected "array", but got "string"#',
            ],
            'Invalid backend size metrics' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'metrics' => [
                        'backend' => [
                            'size' => [],
                        ],
                    ],
                ],
                '#Invalid type for path "job.metrics.backend.size". Expected "scalar", but got "array".#',
            ],
            'Invalid type' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'desiredStatus' => 'processing',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'type' => 'orchestration',
                ],
                '#Invalid configuration for path "job.type": Type must be one of standard, container.#',
            ],
            'Invalid parallelism' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'runId' => '12345',
                    'status' => 'created',
                    'desiredStatus' => 'processing',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'parallelism' => 'more then a little',
                ],
                '#Invalid configuration for path "job.parallelism": ' .
                'Parallelism value must be either null, an integer from range 2-100 or "infinity".#',
            ],
        ];
    }

    /**
     * @dataProvider invalidJobProvider
     * @param array $jobData
     * @param string $message
     */
    public function testInvalidJob(array $jobData, string $message): void
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessageMatches($message);
        $definition = new FullJobDefinition();
        $definition->processData($jobData);
    }

    public function testBackendConfiguration(): void
    {
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'tokenId' => '12345',
            'projectId' => '123',
            'configId' => '123',
            'componentId' => 'keboola.test',
            'mode' => 'run',
            'configRowIds' => ['234'],
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'tag' => 'latest',
            'id' => '1234',
            'runId' => '1234',
            'status' => JobInterface::STATUS_CREATED,
            'desiredStatus' => JobInterface::DESIRED_STATUS_PROCESSING,
            'backend' => [
                'type' => 'my-backend',
                'foo' => 'bar',
            ],
        ];
        $definition = new FullJobDefinition();

        self::assertSame([
            'type' => 'my-backend',
        ], $definition->processData($data)['backend']);
    }

    public function testBehaviorConfiguration(): void
    {
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'tokenId' => '12345',
            'projectId' => '123',
            'configId' => '123',
            'componentId' => 'keboola.test',
            'mode' => 'run',
            'configRowIds' => ['234'],
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'tag' => 'latest',
            'id' => '1234',
            'runId' => '1234',
            'status' => JobInterface::STATUS_CREATED,
            'desiredStatus' => JobInterface::DESIRED_STATUS_PROCESSING,
            'behavior' => [
                'onError' => 'warning',
            ],
        ];
        $definition = new FullJobDefinition();

        self::assertSame([
            'onError' => 'warning',
        ], $definition->processData($data)['behavior']);
    }
}

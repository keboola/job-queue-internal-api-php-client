<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
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
            'status' => JobFactory::STATUS_CREATED,
            'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
        ];
        $definition = new FullJobDefinition();
        $processedData = $definition->processData($expectedData);
        $expectedData['configRowIds'] = [];
        $expectedData['tag'] = null;
        $expectedData['isFinished'] = false;
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
            'status' => JobFactory::STATUS_CREATED,
            'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
            'extraKey' => 'ignored',
            'metrics' => [
                'storage' => [
                    'inputTablesBytesSum' => 123,
                    'storageExtraKey' => 'ignored',
                ],
                'backend' => [
                    'size' => 'medium',
                    'backendExtraKey' => 'ignored',
                ],
            ],
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
                '#Invalid configuration for path "job.mode": Mode must be one of "run" ' .
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
            'status' => JobFactory::STATUS_CREATED,
            'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
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
}

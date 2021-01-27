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
            'result' => [
                'bar' => 'foo',
            ],
            'status' => JobFactory::STATUS_CREATED,
            'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
        ];
        $definition = new FullJobDefinition();
        $processedData = $definition->processData($expectedData);
        $expectedData['configRowId'] = null;
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
            'configRowId' => '234',
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'tag' => 'latest',
            'id' => '1234',
            'status' => JobFactory::STATUS_CREATED,
            'desiredStatus' => JobFactory::DESIRED_STATUS_PROCESSING,
        ];
        $definition = new FullJobDefinition();
        self::assertEquals(array_merge($data, [
            'isFinished' => false,
        ]), $definition->processData($data));
    }

    public function testOrchestratorJob(): void
    {
        $this->markTestSkipped('fix me');
        $expectedData = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'tokenId' => '12345',
            'projectId' => '123',
            'configId' => 12345,
            'orchestration' => [
                'id' => 123456,
                'name' => 'Test orchestration',
            ],
            'componentId' => 'orchestrator',
            'initializedBy' => 'trigger',
            'initiator' => [
                'id' => 199182,
                'description' => 'john.doe@keboola.com',
                'userAgent' => 'my-ua',
            ],
            'notificationsEmails' => [],
            'tasks' => [
                [
                    'phase' => 'New phase',
                    'actionParameters' => [
                        'config' => '554424643',
                    ],
                    'componentId' => 'keboola.ex-db-snowflake',
                    'action' => 'run',
                    'active' => true,
                    'continueOnFailure' => false,
                    'id' => 1234567,
                    'timeoutMinutes' => null,
                ],
            ],
            'id' => '1234',
            'result' => [
                'bar' => 'foo',
            ],
            'status' => 'created',
        ];
        $definition = new FullJobDefinition();
        $processedData = $definition->processData($expectedData);
        $expectedData['params']['configRowId'] = null;
        $expectedData['params']['tag'] = null;
        self::assertEquals($expectedData, $processedData);
    }

    public function invalidJobProvider(): array
    {
        return [
            'Missing token' => [
                [
                    'id' => '12345',
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                'The child node "#tokenString" at path "job" must be configured.',
            ],
            /*
            'Missing componentId' => [
                [
                    'token' => [
                        'token' => getenv('TEST_STORAGE_API_TOKEN'),
                        'id' => '1234',
                    ],
                    'project' => [
                        'id' => '123',
                    ],
                    'id' => '12345',
                    'status' => 'created',
                    'params' => [
                        'configId' => '123',
                        'mode' => 'run',
                    ],
                ],
                'The child node "componentId" at path "job.params" must be configured.',
            ],
            'Missing mode' => [
                [
                    'token' => [
                        'token' => getenv('TEST_STORAGE_API_TOKEN'),
                        'id' => '1234',
                    ],
                    'project' => [
                        'id' => '123',
                    ],
                    'id' => '12345',
                    'status' => 'created',
                    'params' => [
                        'configId' => '123',
                        'componentId' => 'keboola.test',
                    ],
                ],
                'The child node "mode" at path "job.params" must be configured.',
            ],
            */
            'Invalid mode' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'invalid',
                ],
                'Invalid configuration for path "job.mode": Mode must be one of "run" ' .
                'or "debug" (or "dry-run","prepare","input","full","single").',
            ],
            'Invalid configData' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'configData' => '345',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                'Invalid type for path "job.configData". Expected array, but got string',
            ],
            'Invalid configRowId' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'configRowId' => ['123'],
                ],
                'Invalid type for path "job.configRowId". Expected scalar, but got array.',
            ],
            'Invalid tag' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'tag' => ['234'],
                ],
                'Invalid type for path "job.tag". Expected scalar, but got array.',
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
                'The child node "id" at path "job" must be configured.',
            ],
            'Missing status' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                'The child node "status" at path "job" must be configured.',
            ],
            'Invalid status' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'projectId' => '123',
                    'id' => '12345',
                    'status' => 'invalid',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                'Invalid configuration for path "job.status": Status must be one of cancelled, created, error, ' .
                    'processing, success, terminated, terminating, waiting, warning.',
            ],
            'Missing project id' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'tokenId' => '1234',
                    'id' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                'The child node "projectId" at path "job" must be configured.',
            ],
            'Missing token id' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'projectId' => '123',
                    'id' => '12345',
                    'status' => 'created',
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                'The child node "tokenId" at path "job" must be configured.',
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
        self::expectExceptionMessage($message);
        $definition = new FullJobDefinition();
        $definition->processData($jobData);
    }
}

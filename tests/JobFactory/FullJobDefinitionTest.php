<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class FullJobDefinitionTest extends BaseTest
{
    public function testValidJobMaximal(): void
    {
        $expectedData = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
                'id' => '12345',
            ],
            'project' => [
                'id' => '123',
            ],
            'params' => [
                'config' => '123',
                'configData' => [
                    'foo' => 'bar',
                ],
                'component' => 'keboola.test',
                'mode' => 'run',
            ],
            'id' => '1234',
            'result' => [
                'bar' => 'foo',
            ],
            'status' => 'created',
        ];
        $definition = new FullJobDefinition();
        $processedData = $definition->processData($expectedData);
        $expectedData['params']['row'] = null;
        $expectedData['params']['tag'] = null;
        self::assertEquals($expectedData, $processedData);
    }

    public function testValidJobFull(): void
    {
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
                'id' => '12345',
            ],
            'project' => [
                'id' => '123',
            ],
            'params' => [
                'config' => '123',
                'component' => 'keboola.test',
                'mode' => 'run',
                'row' => '234',
                'configData' => [
                    'parameters' => [
                        'foo' => 'bar',
                    ],
                ],
                'tag' => 'latest',
            ],
            'id' => '1234',
            'status' => 'created',
        ];
        $definition = new FullJobDefinition();
        self::assertEquals($data, $definition->processData($data));
    }

    public function invalidJobProvider(): array
    {
        return [
            'Missing token' => [
                [
                    'id' => '12345',
                    'status' => 'created',
                    'params' => [
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                    ],
                ],
                'The child node "token" at path "job" must be configured.',
            ],
            'Missing component' => [
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
                        'config' => '123',
                        'mode' => 'run',
                    ],
                ],
                'The child node "component" at path "job.params" must be configured.',
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
                        'config' => '123',
                        'component' => 'keboola.test',
                    ],
                ],
                'The child node "mode" at path "job.params" must be configured.',
            ],
            'Invalid mode' => [
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
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'invalid',
                    ],
                ],
                'Invalid configuration for path "job.params.mode": Mode must be one of "run" or "debug".',
            ],
            'Missing params' => [
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
                ],
                'The child node "params" at path "job" must be configured.',
            ],
            'Invalid configData' => [
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
                        'config' => '123',
                        'configData' => '345',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                    ],
                ],
                'Invalid type for path "job.params.configData". Expected array, but got string',
            ],
            'Invalid row' => [
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
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                        'row' => ['123'],
                    ],
                ],
                'Invalid type for path "job.params.row". Expected scalar, but got array.',
            ],
            'Invalid tag' => [
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
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                        'tag' => ['234'],
                    ],
                ],
                'Invalid type for path "job.params.tag". Expected scalar, but got array.',
            ],
            'Missing id' => [
                [
                    'token' => [
                        'token' => getenv('TEST_STORAGE_API_TOKEN'),
                        'id' => '1234',
                    ],
                    'project' => [
                        'id' => '123',
                    ],
                    'status' => 'created',
                    'params' => [
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                    ],
                ],
                'The child node "id" at path "job" must be configured.',
            ],
            'Missing status' => [
                [
                    'token' => [
                        'token' => getenv('TEST_STORAGE_API_TOKEN'),
                        'id' => '1234',
                    ],
                    'project' => [
                        'id' => '123',
                    ],
                    'id' => '12345',
                    'params' => [
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                    ],
                ],
                'The child node "status" at path "job" must be configured.',
            ],
            'Invalid status' => [
                [
                    'token' => [
                        'token' => getenv('TEST_STORAGE_API_TOKEN'),
                        'id' => '1234',
                    ],
                    'project' => [
                        'id' => '123',
                    ],
                    'id' => '12345',
                    'status' => 'invalid',
                    'params' => [
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                    ],
                ],
                'Invalid configuration for path "job.status": Status must be one of cancelled, created, error, ' .
                    'processing, success, terminated, terminating, waiting, warning.',
            ],
            'Missing project id' => [
                [
                    'token' => [
                        'token' => getenv('TEST_STORAGE_API_TOKEN'),
                        'id' => '1234',
                    ],
                    'id' => '12345',
                    'status' => 'created',
                    'params' => [
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                    ],
                ],
                'The child node "project" at path "job" must be configured.',
            ],
            'Missing token id' => [
                [
                    'token' => [
                        'token' => getenv('TEST_STORAGE_API_TOKEN'),
                    ],
                    'project' => [
                        'id' => '123',
                    ],
                    'id' => '12345',
                    'status' => 'created',
                    'params' => [
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                    ],
                ],
                'The child node "id" at path "job.token" must be configured.',
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

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class NewJobDefinitionTest extends TestCase
{
    public function testValidJobMinimal(): void
    {
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '123',
                'component' => 'keboola.test',
                'mode' => 'run',
            ],
        ];
        $definition = new NewJobDefinition();
        self::assertEquals($data, $definition->processData($data));
    }

    public function testValidJobFull(): void
    {
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
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
        ];
        $definition = new NewJobDefinition();
        self::assertEquals($data, $definition->processData($data));
    }

    public function invalidJobProvider(): array
    {
        return [
            'Missing token' => [
                [
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
                    ],
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
                    ],
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
                    ],
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
                    ],
                ],
                'The child node "params" at path "job" must be configured.',
            ],
            'Invalid configData' => [
                [
                    'token' => [
                        'token' => getenv('TEST_STORAGE_API_TOKEN'),
                    ],
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
                    ],
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
                    ],
                    'params' => [
                        'config' => '123',
                        'component' => 'keboola.test',
                        'mode' => 'run',
                        'tag' => ['234'],
                    ],
                ],
                'Invalid type for path "job.params.tag". Expected scalar, but got array.',
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
        $definition = new NewJobDefinition();
        $definition->processData($jobData);
    }
}
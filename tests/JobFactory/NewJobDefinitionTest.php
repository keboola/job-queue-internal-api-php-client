<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class NewJobDefinitionTest extends BaseTest
{
    public function testValidJobMinimal(): void
    {
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => '123',
            'component' => 'keboola.test',
            'mode' => 'run',
        ];
        $definition = new NewJobDefinition();
        self::assertEquals($data, $definition->processData($data));
    }

    public function testValidJobFull(): void
    {
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
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
        ];
        $definition = new NewJobDefinition();
        self::assertEquals($data, $definition->processData($data));
    }

    public function invalidJobProvider(): array
    {
        return [
            'Missing token' => [
                [
                    'config' => '123',
                    'component' => 'keboola.test',
                    'mode' => 'run',
                ],
                'The child node "token" at path "job" must be configured.',
            ],
            'Missing component' => [
                [
                    'token' => getenv('TEST_STORAGE_API_TOKEN'),
                    'config' => '123',
                    'mode' => 'run',
                ],
                'The child node "component" at path "job" must be configured.',
            ],
            'Missing mode' => [
                [
                    'token' => getenv('TEST_STORAGE_API_TOKEN'),
                    'config' => '123',
                    'component' => 'keboola.test',
                ],
                'The child node "mode" at path "job" must be configured.',
            ],
            'Invalid mode' => [
                [
                    'token' => getenv('TEST_STORAGE_API_TOKEN'),
                    'config' => '123',
                    'component' => 'keboola.test',
                    'mode' => 'invalid',
                ],
                'Invalid configuration for path "job.mode": Mode must be one of "run" or "debug".',
            ],
                'Invalid configData' => [
                [
                    'token' => getenv('TEST_STORAGE_API_TOKEN'),
                    'config' => '123',
                    'configData' => '345',
                    'component' => 'keboola.test',
                    'mode' => 'run',
                ],
                'Invalid type for path "job.configData". Expected array, but got string',
            ],
            'Invalid row' => [
                [
                    'token' => getenv('TEST_STORAGE_API_TOKEN'),
                    'config' => '123',
                    'component' => 'keboola.test',
                    'mode' => 'run',
                    'row' => ['123'],
                ],
                'Invalid type for path "job.row". Expected scalar, but got array.',
            ],
            'Invalid tag' => [
                [
                    'token' => getenv('TEST_STORAGE_API_TOKEN'),
                    'config' => '123',
                    'component' => 'keboola.test',
                    'mode' => 'run',
                    'tag' => ['234'],
                ],
                'Invalid type for path "job.tag". Expected scalar, but got array.',
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

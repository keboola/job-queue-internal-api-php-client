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
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '123',
            'componentId' => 'keboola.test',
            'result' => [],
        ];
        $definition = new NewJobDefinition();
        $processed = $definition->processData($data);
        self::assertEquals(
            array_merge($data, [
                'mode' => 'run',
                'result' => [],
                'configRowIds' => [],
            ]),
            $processed
        );
    }

    public function testValidJobFull(): void
    {
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '12345',
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
            'result' => ['foo' => 'bar'],
        ];
        $definition = new NewJobDefinition();
        self::assertEquals($data, $definition->processData($data));
    }

    public function invalidJobProvider(): array
    {
        return [
            'Missing token' => [
                [
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#The child (node|config) "\#tokenString" (at path|under) "job" must be configured.#',
            ],
            'Missing componentId' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'mode' => 'run',
                ],
                '#The child (node|config) "componentId" (at path|under) "job" must be configured.#',
            ],
            'Invalid mode' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'invalid',
                ],
                '#Invalid configuration for path "job.mode": Mode must be one of "run" or "debug".#',
            ],
            'Invalid configData' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'configData' => '345',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                ],
                '#Invalid type for path "job.configData". Expected "?array"?, but got "?string"?#',
            ],
            'Invalid row' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
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
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'tag' => ['234'],
                ],
                '#Invalid type for path "job.tag". Expected "?scalar"?, but got "?array"?.#',
            ],
            'Invalid result' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'tag' => '234',
                    'result' => 'invalid',
                ],
                '#Invalid type for path "job.result". Expected "?array"?, but got "?string"?#',
            ],
            'Invalid run id' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'parentRunId' => ['123', '345'],
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'mode' => 'run',
                    'tag' => ['234'],
                ],
                '#Invalid type for path "job.parentRunId". Expected "?scalar"?, but got "?array"?.#',
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
        $definition = new NewJobDefinition();
        $definition->processData($jobData);
    }
}

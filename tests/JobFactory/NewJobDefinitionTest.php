<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Generator;
use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Keboola\JobQueueInternalClient\Tests\BaseTest;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class NewJobDefinitionTest extends BaseTest
{
    public function validModeData(): Generator
    {
        yield 'debug mode' => ['debug'];
        yield 'run mode' => ['run'];
        yield 'forceRun mode' => ['forceRun'];
    }

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
            $processed,
        );
    }

    /**
     * @dataProvider validModeData
     */
    public function testValidJobFull(string $mode): void
    {
        $data = [
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '12345',
            'configId' => '123',
            'componentId' => 'keboola.test',
            'mode' => $mode,
            'configRowIds' => ['234'],
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
                ],
            ],
            'tag' => 'latest',
            'result' => ['foo' => 'bar'],
            'backend' => [
                'type' => 'my',
                'containerType' => 'his',
                'context' => 'wml',
                'extraKey' => 'ignored',
            ],
            'orchestrationJobId' => '123456789',
            'orchestrationTaskId' => '123',
            'onlyOrchestrationTaskIds' => ['45', '67'],
            'previousJobId' => '789',
        ];
        $definition = new NewJobDefinition();
        $result = $definition->processData($data);
        unset($data['backend']['extraKey']);
        self::assertSame($data, $result);
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
                '#Invalid configuration for path "job.mode": Mode must be one of "run", "forceRun" or "debug".#',
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
            'orchestrationTaskId not string' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'orchestrationTaskId' => 134,
                ],
                '/Invalid configuration for path "job.orchestrationTaskId": value must be a string/',
            ],
            'orchestrationTaskId empty string' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'orchestrationTaskId' => '',
                ],
                '/Invalid configuration for path "job.orchestrationTaskId": value cannot be empty string/',
            ],
            'onlyOrchestrationTaskIds not list' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'onlyOrchestrationTaskIds' => '123',
                ],
                '/Invalid configuration for path "job.onlyOrchestrationTaskIds": value must be an array/',
            ],
            'onlyOrchestrationTaskIds empty list' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'onlyOrchestrationTaskIds' => [],
                ],
                '/Invalid configuration for path "job.onlyOrchestrationTaskIds": value cannot be empty list/',
            ],
            'onlyOrchestrationTaskIds with non-string item' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'onlyOrchestrationTaskIds' => [123],
                ],
                '/Invalid configuration for path "job.onlyOrchestrationTaskIds": items must be strings/',
            ],
            'onlyOrchestrationTaskIds with null item' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'onlyOrchestrationTaskIds' => [null],
                ],
                '/Invalid configuration for path "job.onlyOrchestrationTaskIds": items must be strings/',
            ],
            'onlyOrchestrationTaskIds with empty item' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'onlyOrchestrationTaskIds' => [''],
                ],
                '/Invalid configuration for path "job.onlyOrchestrationTaskIds": item cannot be empty string/',
            ],
            'onlyOrchestrationTaskIds with duplicated item' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'onlyOrchestrationTaskIds' => ['11', '22', '11'],
                ],
                '/Invalid configuration for path "job.onlyOrchestrationTaskIds": items must be unique/',
            ],
            'previousJobId not string' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'previousJobId' => 134,
                ],
                '/Invalid configuration for path "job.previousJobId": value must be a string/',
            ],
            'previousJobId empty string' => [
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configId' => '123',
                    'componentId' => 'keboola.test',
                    'result' => [],
                    'previousJobId' => '',
                ],
                '/Invalid configuration for path "job.previousJobId": value cannot be empty string/',
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

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\ElasticJobDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ElasticJobDefinitionTest extends TestCase
{
    public static function provideValidConfiguration(): iterable
    {
        yield 'minimal configuration' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
            ],
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'configRowIds' => [],
                'tag' => null,
                'isFinished' => false,
                'type' => 'standard',
                'parallelism' => null,
                'orchestrationJobId' => null,
                'runnerId' => null,
            ],
        ];

        yield 'complete configuration' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'parentRunId' => '54321',
                'projectId' => '123',
                'projectName' => 'Test Project',
                'tokenId' => '1234',
                'tokenDescription' => 'Test Token',
                'componentId' => 'keboola.test',
                'configId' => '456',
                'configRowIds' => ['789', '101112'],
                'configData' => ['foo' => 'bar'],
                'tag' => 'latest',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'mode' => 'run',
                'type' => 'standard',
                'parallelism' => null,
                'result' => ['bar' => 'foo'],
                'usageData' => ['data' => 'usage'],
                'isFinished' => false,
                'url' => 'https://example.com',
                'branchId' => '789',
                'branchType' => 'dev',
                'variableValuesId' => '101112',
                'variableValuesData' => [
                    'values' => [
                        ['name' => 'var1', 'value' => 'val1'],
                        ['name' => 'var2', 'value' => 'val2'],
                    ],
                ],
                'backend' => [
                    'type' => 'large',
                    'containerType' => 'small',
                    'context' => 'wml',
                ],
                'executor' => 'dind',
                'metrics' => [
                    'storage' => [
                        'inputTablesBytesSum' => 123,
                        'outputTablesBytesSum' => 456,
                    ],
                    'backend' => [
                        'size' => 'medium',
                        'containerSize' => 'large',
                        'context' => 'wml',
                    ],
                ],
                'orchestrationJobId' => '123456789',
                'orchestrationTaskId' => '123',
                'orchestrationPhaseId' => '951',
                'onlyOrchestrationTaskIds' => ['45', 67],
                'previousJobId' => '789',
                'runnerId' => '13579',
                'behavior' => [
                    'onError' => 'warning',
                ],
                'extraKey' => 'ignored',
            ],
            [
                'id' => '12345',
                'runId' => '12345',
                'parentRunId' => '54321',
                'projectId' => '123',
                'projectName' => 'Test Project',
                'tokenId' => '1234',
                'tokenDescription' => 'Test Token',
                'componentId' => 'keboola.test',
                'configId' => '456',
                'configRowIds' => ['789', '101112'],
                'configData' => ['foo' => 'bar'],
                'tag' => 'latest',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'mode' => 'run',
                'type' => 'standard',
                'parallelism' => null,
                'result' => ['bar' => 'foo'],
                'usageData' => ['data' => 'usage'],
                'isFinished' => false,
                'url' => 'https://example.com',
                'branchId' => '789',
                'branchType' => 'dev',
                'variableValuesId' => '101112',
                'variableValuesData' => [
                    'values' => [
                        ['name' => 'var1', 'value' => 'val1'],
                        ['name' => 'var2', 'value' => 'val2'],
                    ],
                ],
                'backend' => [
                    'type' => 'large',
                    'containerType' => 'small',
                    'context' => 'wml',
                ],
                'executor' => 'dind',
                'metrics' => [
                    'storage' => [
                        'inputTablesBytesSum' => 123,
                        'outputTablesBytesSum' => 456,
                    ],
                    'backend' => [
                        'size' => 'medium',
                        'containerSize' => 'large',
                        'context' => 'wml',
                    ],
                ],
                'orchestrationJobId' => '123456789',
                'orchestrationTaskId' => '123',
                'orchestrationPhaseId' => '951',
                'onlyOrchestrationTaskIds' => ['45', 67],
                'previousJobId' => '789',
                'runnerId' => '13579',
                'behavior' => [
                    'onError' => 'warning',
                ],
            ],
        ];

        yield 'numeric values converted to strings' => [
            [
                'id' => 12345,
                'runId' => 12345,
                'projectId' => 123,
                'tokenId' => 1234,
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'configId' => 456,
                'branchId' => 789,
            ],
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'configId' => '456',
                'branchId' => '789',
                'configRowIds' => [],
                'tag' => null,
                'isFinished' => false,
                'type' => 'standard',
                'parallelism' => null,
                'orchestrationJobId' => null,
                'runnerId' => null,
            ],
        ];
    }

    /** @dataProvider provideValidConfiguration */
    public function testValidConfiguration(array $data, array $expectedResult): void
    {
        $definition = new ElasticJobDefinition();
        $result = $definition->processData($data);

        self::assertEquals($expectedResult, $result);
    }

    public static function provideInvalidConfiguration(): iterable
    {
        yield 'missing id' => [
            [
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
            ],
            'The child config "id" under "job" must be configured.',
        ];

        yield 'missing runId' => [
            [
                'id' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
            ],
            'The child config "runId" under "job" must be configured.',
        ];

        yield 'missing projectId' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
            ],
            'The child config "projectId" under "job" must be configured.',
        ];

        yield 'missing tokenId' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
            ],
            'The child config "tokenId" under "job" must be configured.',
        ];

        yield 'missing componentId' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'status' => 'created',
                'desiredStatus' => 'processing',
            ],
            'The child config "componentId" under "job" must be configured.',
        ];

        yield 'missing status' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'desiredStatus' => 'processing',
            ],
            'The child config "status" under "job" must be configured.',
        ];

        yield 'missing desiredStatus' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
            ],
            'The child config "desiredStatus" under "job" must be configured.',
        ];

        yield 'invalid status' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'invalid',
                'desiredStatus' => 'processing',
            ],
            'Invalid configuration for path "job.status": Status must be one of cancelled, created, error, ' .
            'processing, success, terminated, terminating, waiting, warning.',
        ];

        yield 'invalid desiredStatus' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'invalid',
            ],
            'Invalid configuration for path "job.desiredStatus": DesiredStatus must be one of processing, terminating.',
        ];

        yield 'invalid mode' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'mode' => 'invalid',
            ],
            'Invalid configuration for path "job.mode": Mode must be one of "run", "forceRun" or "debug" ' .
            '(or "dry-run","prepare","input","full","single").',
        ];

        yield 'invalid tag' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'mode' => 'invalid',
                'tag' => ['234'],
            ],
            'Invalid type for path "job.tag". Expected "scalar", but got "array".',
        ];

        yield 'invalid type' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'type' => 'invalid',
            ],
            'The value "invalid" is not allowed for path "job.type". Permissible values: "standard", "container", ' .
            '"phaseContainer", "orchestrationContainer"',
        ];

        yield 'invalid parallelism' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'parallelism' => 'invalid',
            ],
            'Invalid configuration for path "job.parallelism": Parallelism value must be either null, an integer ' .
            'from range 2-100 or "infinity".',
        ];

        yield 'invalid branchType' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'branchType' => 'invalid',
            ],
            'The value "invalid" is not allowed for path "job.branchType". Permissible values: "default", "dev"',
        ];

        yield 'invalid executor' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'executor' => 'invalid',
            ],
            'The value "invalid" is not allowed for path "job.executor". Permissible values: ' .
            'null, "dind", "k8sContainers".',
        ];

        yield 'invalid deduplicationId - empty value' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'deduplicationId' => '',
            ],
            'Invalid configuration for path "job.deduplicationId": value cannot be empty string',
        ];

        yield 'invalid deduplicationId - non-string value' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'deduplicationId' => 7,
            ],
            'Invalid configuration for path "job.deduplicationId": value must be a string',
        ];

        yield 'invalid orchestrationTaskId - empty string' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'orchestrationTaskId' => '',
            ],
            'Invalid configuration for path "job.orchestrationTaskId": value cannot be empty string',
        ];

        yield 'invalid orchestrationTaskId - non-string value' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'orchestrationTaskId' => 123,
            ],
            'Invalid configuration for path "job.orchestrationTaskId": value must be a string',
        ];

        yield 'invalid orchestrationPhaseId - empty string' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'orchestrationPhaseId' => '',
            ],
            'Invalid configuration for path "job.orchestrationPhaseId": value cannot be empty string',
        ];

        yield 'invalid orchestrationPhaseId - non-string value' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'orchestrationPhaseId' => 123,
            ],
            'Invalid configuration for path "job.orchestrationPhaseId": value must be a string',
        ];

        yield 'invalid onlyOrchestrationTaskIds - not an array' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'onlyOrchestrationTaskIds' => 'invalid',
            ],
            'Invalid configuration for path "job.onlyOrchestrationTaskIds": value must be an array',
        ];

        yield 'invalid onlyOrchestrationTaskIds - empty array' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'onlyOrchestrationTaskIds' => [],
            ],
            'Invalid configuration for path "job.onlyOrchestrationTaskIds": value cannot be empty list',
        ];

        yield 'invalid onlyOrchestrationTaskIds - non-scalar items' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'onlyOrchestrationTaskIds' => [[]],
            ],
            'Invalid configuration for path "job.onlyOrchestrationTaskIds": items must be scalars',
        ];

        yield 'invalid onlyOrchestrationTaskIds - empty string item' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'onlyOrchestrationTaskIds' => [''],
            ],
            'Invalid configuration for path "job.onlyOrchestrationTaskIds": item cannot be empty string',
        ];

        yield 'invalid onlyOrchestrationTaskIds - duplicate items' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'onlyOrchestrationTaskIds' => ['123', '123'],
            ],
            'Invalid configuration for path "job.onlyOrchestrationTaskIds": items must be unique',
        ];

        yield 'invalid previousJobId - empty string' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'previousJobId' => '',
            ],
            'Invalid configuration for path "job.previousJobId": value cannot be empty string',
        ];

        yield 'invalid previousJobId - non-string value' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'previousJobId' => 123,
            ],
            'Invalid configuration for path "job.previousJobId": value must be a string',
        ];

        yield 'invalid variableValuesData - invalid values item' => [
            [
                'id' => '12345',
                'runId' => '12345',
                'projectId' => '123',
                'tokenId' => '1234',
                'componentId' => 'keboola.test',
                'status' => 'created',
                'desiredStatus' => 'processing',
                'variableValuesData' => [
                    'values' => [
                        ['name' => 'var1'],
                    ],
                ],
            ],
            'The child config "value" under "job.variableValuesData.values.0" must be configured.',
        ];
    }

    /** @dataProvider provideInvalidConfiguration */
    public function testInvalidConfiguration(array $data, string $expectedError): void
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage($expectedError);

        $definition = new ElasticJobDefinition();
        $definition->processData($data);
    }

    public function testExtraKeysAreIgnored(): void
    {
        $data = [
            'id' => '12345',
            'runId' => '12345',
            'projectId' => '123',
            'tokenId' => '1234',
            'componentId' => 'keboola.test',
            'status' => 'created',
            'desiredStatus' => 'processing',
            'behavior' => [
                'onError' => 'warning',
                'extraKey' => 'ignored',
            ],
            'backend' => [
                'type' => 'large',
                'extraKey' => 'ignored',
            ],
            'metrics' => [
                'storage' => [
                    'inputTablesBytesSum' => 123,
                ],
                'backend' => [
                    'size' => 'medium',
                    'extraKey' => 'ignored',
                ],
                'extraKey' => 'ignored',
            ],
        ];

        $definition = new ElasticJobDefinition();
        $result = $definition->processData($data);

        self::assertArrayHasKey('behavior', $result);
        self::assertArrayHasKey('onError', $result['behavior']);
        self::assertArrayNotHasKey('extraKey', $result['behavior']);

        self::assertArrayHasKey('backend', $result);
        self::assertArrayHasKey('type', $result['backend']);
        self::assertArrayNotHasKey('extraKey', $result['backend']);

        self::assertArrayHasKey('metrics', $result);
        self::assertArrayHasKey('storage', $result['metrics']);
        self::assertArrayHasKey('inputTablesBytesSum', $result['metrics']['storage']);
        self::assertArrayNotHasKey('extraKey', $result['metrics']['storage']);

        self::assertArrayHasKey('backend', $result['metrics']);
        self::assertArrayHasKey('size', $result['metrics']['backend']);
        self::assertArrayNotHasKey('extraKey', $result['metrics']['backend']);
        self::assertArrayNotHasKey('extraKey', $result['metrics']);
    }

    public function testStringNormalizerWithScalarValues(): void
    {
        $data = [
            'id' => 12345, // Integer
            'runId' => 12345.67, // Float
            'projectId' => true, // Boolean
            'tokenId' => '1234', // String
            'componentId' => 'keboola.test',
            'status' => 'created',
            'desiredStatus' => 'processing',
            'configId' => 0, // Zero
            'branchId' => '', // Empty string
        ];

        $definition = new ElasticJobDefinition();
        $result = $definition->processData($data);

        // Integers, floats, and booleans should be converted to strings
        self::assertSame('12345', $result['id']);
        self::assertSame('12345.67', $result['runId']);
        self::assertSame('1', $result['projectId']); // true becomes "1"
        self::assertSame('1234', $result['tokenId']);
        self::assertSame(null, $result['configId']);
        self::assertNull($result['branchId']); // Empty string becomes null
    }
}

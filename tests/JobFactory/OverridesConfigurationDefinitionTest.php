<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\OverridesConfigurationDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class OverridesConfigurationDefinitionTest extends TestCase
{
    public function testValidOverrideMinimal(): void
    {
        $data = [];
        $definition = new OverridesConfigurationDefinition();
        $processed = $definition->processData($data);
        self::assertEquals([], $processed);
    }

    public function testValidOverrideFull(): void
    {
        $data = [
            'extra' => 'removed',
            'variableValuesData' => [
                'values' => [
                    [
                        'name' => 'foo',
                        'value' => 'bar',
                    ],
                ],
                'extra' => 'removed',
            ],
            'variableValuesId' => 123,
            'runtime' => [
                'also' => 'removed',
                'tag' => 1,
                'backend' => [
                    'type' => 'weird',
                    'context' => 'wml',
                    'ignored' => 'yes',
                ],
            ],
        ];
        $definition = new OverridesConfigurationDefinition();
        $expected = $data;
        unset($expected['extra']);
        unset($expected['runtime']['also']);
        unset($expected['runtime']['backend']['ignored']);
        unset($expected['variableValuesData']['extra']);
        $expected['variableValuesId'] = '123';
        $expected['runtime']['tag'] = '1';
        $expected['runtime']['process_timeout'] = null;
        $expected['runtime']['parallelism'] = null;
        self::assertSame($expected, $definition->processData($data));
    }

    public function invalidConfigurationProvider(): iterable
    {
        // phpcs:disable Generic.Files.LineLength
        yield 'Invalid Variable Values Data' => [
            'jobData' => [
                'variableValuesData' => '5',
            ],
            'message' => '#Invalid type for path "overrides.variableValuesData". Expected "?array"?, but got "?string"?#',
        ];

        yield 'Invalid variable values ID' => [
            'jobData' => [
                'variableValuesId' => ['123'],
                'runtime' => [
                    'tag' => '1.2.3',
                ],
            ],
            'message' => '#Invalid type for path "overrides.variableValuesId". Expected "?scalar"?, but got "?array"?#',
        ];

        yield 'Invalid tag' => [
            'jobData' => [
                'runtime' => [
                    'tag' => ['1.2.3'],
                ],
            ],
            'message' => '#Invalid type for path "overrides.runtime.tag". Expected "?scalar"?, but got "?array"?#',
        ];

        yield 'Invalid backend' => [
            'jobData' => [
                'runtime' => [
                    'backend' => [
                        'type' => ['weird'],
                    ],
                ],
            ],
            'message' => '#Invalid type for path "overrides.runtime.backend.type". Expected "?scalar"?, but got "?array"?#',
        ];

        yield 'Invalid executor' => [
            'jobData' => [
                'runtime' => [
                    'executor' => 'foo',
                ],
            ],
            'message' => '#The value "foo" is not allowed for path "overrides.runtime.executor". Permissible values: null, "dind", "k8sContainers"#',
        ];

        yield 'process_timeout zero' => [
            'jobData' => [
                'runtime' => [
                    'process_timeout' => 0,
                ],
            ],
            'message' => '#^Invalid configuration for path "overrides.runtime.process_timeout": must be greater than 0$#',
        ];

        yield 'process_timeout negative' => [
            'jobData' => [
                'runtime' => [
                    'process_timeout' => -10,
                ],
            ],
            'message' => '#^Invalid configuration for path "overrides.runtime.process_timeout": must be greater than 0$#',
        ];

        yield 'process_timeout float' => [
            'jobData' => [
                'runtime' => [
                    'process_timeout' => 10.0,
                ],
            ],
            'message' => '#^Invalid type for path "overrides.runtime.process_timeout". Expected "int", but got "float".$#',
        ];
        // phpcs:enable Generic.Files.LineLength
    }

    /** @dataProvider invalidConfigurationProvider */
    public function testInvalidConfigurationOverride(array $jobData, string $message): void
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessageMatches($message);
        $definition = new OverridesConfigurationDefinition();
        $definition->processData($jobData);
    }
}

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
            ],
            'variableValuesId' => 123,
            'runtime' => [
                'also' => 'removed',
                'tag' => 1,
                'backend' => [
                    'type' => 'weird',
                ],
            ],
        ];
        $definition = new OverridesConfigurationDefinition();
        $expected = $data;
        unset($expected['extra']);
        unset($expected['runtime']['also']);
        $expected['variableValuesId'] = '123';
        $expected['runtime']['tag'] = '1';
        self::assertSame($expected, $definition->processData($data));
    }

    public function invalidConfigurationProvider(): array
    {
        return [
            'Invalid Variable Values Data' => [
                [
                    'variableValuesData' => '5',
                ],
                '#Invalid type for path "overrides.variableValuesData". Expected "?array"?, but got "?string"?#',
            ],
            'Invalid variable values ID' => [
                [
                    'variableValuesId' => ['123'],
                    'runtime' => [
                        'tag' => '1.2.3',
                    ],
                ],
                '#Invalid type for path "overrides.variableValuesId". Expected "?scalar"?, but got "?array"?#',
            ],
            'Invalid tag' => [
                [
                    'runtime' => [
                        'tag' => ['1.2.3'],
                    ],
                ],
                '#Invalid type for path "overrides.runtime.tag". Expected "?scalar"?, but got "?array"?#',
            ],
            'Invalid backend' => [
                [
                    'runtime' => [
                        'backend' => [
                            'type' => ['weird'],
                        ],
                    ],
                ],
                '#Invalid type for path "overrides.runtime.backend.type". Expected "?scalar"?, but got "?array"?#',
            ],
        ];
    }

    /**
     * @dataProvider invalidConfigurationProvider
     * @param array $jobData
     * @param string $message
     */
    public function testInvalidConfigurationOverride(array $jobData, string $message): void
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessageMatches($message);
        $definition = new OverridesConfigurationDefinition();
        $definition->processData($jobData);
    }
}

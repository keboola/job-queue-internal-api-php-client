<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\Configuration;

use Generator;
use Keboola\JobQueueInternalClient\JobFactory\Configuration\ComponentDefinition;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ComponentDefinitionTest extends TestCase
{
    /**
     * @dataProvider validDataProvider
     */
    public function testValid(array $data, array $expected): void
    {
        $definition = new ComponentDefinition();
        $result = $definition->processData($data);
        self::assertSame($expected, $result);
    }

    public function validDataProvider(): Generator
    {
        //phpcs:disable Generic.Files.LineLength.MaxExceeded
        yield 'minimal' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-config-test',
                        'type' => 'aws-ecr',
                    ],
                ],
            ],
            'expected' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-config-test',
                        'type' => 'aws-ecr',
                        'tag' => 'latest',
                        'digest' => '',
                    ],
                    'memory' => '256m',
                    'configuration_format' => 'json',
                    'process_timeout' => 3600,
                    'forward_token' => false,
                    'forward_token_details' => false,
                    'default_bucket' => false,
                    'image_parameters' => [],
                    'network' => 'bridge',
                    'default_bucket_stage' => 'in',
                    'synchronous_actions' => [],
                    'logging' => [
                        'type' => 'standard',
                        'verbosity' => [
                            100 => 'none',
                            200 => 'normal',
                            250 => 'normal',
                            300 => 'normal',
                            400 => 'normal',
                            500 => 'camouflage',
                            550 => 'camouflage',
                            600 => 'camouflage',
                        ],
                        'gelf_server_type' => 'tcp',
                        'no_application_errors' => false,
                    ],
                    'staging_storage' => [
                        'input' => 'local',
                        'output' => 'local',
                    ],
                ],
                'features' => [],
            ],
        ];
        yield 'maximal' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-config-test',
                        'type' => 'aws-ecr',
                        'tag' => '1.2.3',
                        'digest' => '',
                    ],
                    'memory' => '512m',
                    'configuration_format' => 'json',
                    'process_timeout' => 1200,
                    'forward_token' => true,
                    'forward_token_details' => true,
                    'default_bucket' => true,
                    'image_parameters' => [
                        'foo' => 'bar',
                    ],
                    'network' => 'none',
                    'default_bucket_stage' => 'out',
                    'synchronous_actions' => [
                        'test',
                    ],
                    'logging' => [
                        'type' => 'standard',
                        'verbosity' => [
                            100 => 'verbose',
                            200 => 'verbose',
                            250 => 'normal',
                            300 => 'normal',
                            400 => 'camouflage',
                            500 => 'camouflage',
                            550 => 'camouflage',
                            600 => 'none',
                        ],
                        'gelf_server_type' => 'udp',
                        'no_application_errors' => true,
                    ],
                    'vendor' => [
                        'we' => 'do not',
                        'care' => 'at all',
                    ],
                    'staging_storage' => [
                        'input' => 's3',
                        'output' => 'workspace-snowflake',
                    ],
                ],
                'features' => [
                    'some-feature',
                ],
            ],
            'expected' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-config-test',
                        'type' => 'aws-ecr',
                        'tag' => '1.2.3',
                        'digest' => '',
                    ],
                    'memory' => '512m',
                    'configuration_format' => 'json',
                    'process_timeout' => 1200,
                    'forward_token' => true,
                    'forward_token_details' => true,
                    'default_bucket' => true,
                    'image_parameters' => [
                        'foo' => 'bar',
                    ],
                    'network' => 'none',
                    'default_bucket_stage' => 'out',
                    'synchronous_actions' => [
                        'test',
                    ],
                    'logging' => [
                        'type' => 'standard',
                        'verbosity' => [
                            100 => 'verbose',
                            200 => 'verbose',
                            250 => 'normal',
                            300 => 'normal',
                            400 => 'camouflage',
                            500 => 'camouflage',
                            550 => 'camouflage',
                            600 => 'none',
                        ],
                        'gelf_server_type' => 'udp',
                        'no_application_errors' => true,
                    ],
                    'vendor' => [
                        'we' => 'do not',
                        'care' => 'at all',
                    ],
                    'staging_storage' => [
                        'input' => 's3',
                        'output' => 'workspace-snowflake',
                    ],
                ],
                'features' => [
                    'some-feature',
                ],
            ],
        ];
        yield 'other options' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-config-test',
                        'type' => 'aws-ecr',
                    ],
                    'default_bucket_stage' => 'in',
                    'logging' => [
                        'type' => 'standard',
                        'gelf_server_type' => 'tcp',
                        'no_application_errors' => true,
                    ],
                ],
            ],
            'expected' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-config-test',
                        'type' => 'aws-ecr',
                        'tag' => 'latest',
                        'digest' => '',
                    ],
                    'default_bucket_stage' => 'in',
                    'logging' => [
                        'type' => 'standard',
                        'gelf_server_type' => 'tcp',
                        'no_application_errors' => true,
                        'verbosity' => [
                            100 => 'none',
                            200 => 'normal',
                            250 => 'normal',
                            300 => 'normal',
                            400 => 'normal',
                            500 => 'camouflage',
                            550 => 'camouflage',
                            600 => 'camouflage',
                        ],
                    ],
                    'memory' => '256m',
                    'configuration_format' => 'json',
                    'process_timeout' => 3600,
                    'forward_token' => false,
                    'forward_token_details' => false,
                    'default_bucket' => false,
                    'image_parameters' => [],
                    'network' => 'bridge',
                    'synchronous_actions' => [],
                    'staging_storage' => [
                        'input' => 'local',
                        'output' => 'local',
                    ],
                ],
                'features' => [],
            ],
        ];
        //phpcs:enable Generic.Files.LineLength.MaxExceeded
    }

    /**
     * @dataProvider invalidDataProvider
     */
    public function testInvalid(array $data, string $expectedMessage): void
    {
        $definition = new ComponentDefinition();
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage($expectedMessage);
        $definition->processData($data);
    }

    public function invalidDataProvider(): Generator
    {
        yield 'missing id' => [
            'data' => [
            ],
            'expected' => 'The child config "id" under "component" must be configured.',
        ];
        yield 'missing data' => [
            'data' => [
                'id' => 'test',
            ],
            'expected' => 'The child config "data" under "component" must be configured.',
        ];
        yield 'missing definition' => [
            'data' => [
                'id' => 'test',
                'data' => [],
            ],
            'expected' => 'The child config "definition" under "component.data" must be configured.',
        ];
        yield 'missing definition type' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                    ],
                ],
            ],
            'expected' => 'The child config "type" under "component.data.definition" must be configured.',
        ];
        yield 'missing definition uri' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'aws-ecr',
                    ],
                ],
            ],
            'expected' => 'The child config "uri" under "component.data.definition" must be configured.',
        ];
        yield 'invalid definition type' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'invalid',
                    ],
                ],
            ],
            'expected' => 'Invalid configuration for path ' .
                '"component.data.definition.type": Invalid image type "invalid".',
        ];
        yield 'invalid network' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'aws-ecr',
                        'uri' => 'some-uri',
                    ],
                    'network' => 'invalid',
                ],
            ],
            'expected' => 'Invalid configuration for path "component.data.network": Invalid network type "invalid".',
        ];
        yield 'invalid stage' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'aws-ecr',
                        'uri' => 'some-uri',
                    ],
                    'default_bucket_stage' => 'invalid',
                ],
            ],
            'expected' => 'Invalid configuration for path ' .
                '"component.data.default_bucket_stage": Invalid default_bucket_stage "invalid".',
        ];
        yield 'invalid logging type' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'aws-ecr',
                        'uri' => 'some-uri',
                    ],
                    'logging' => [
                        'type' => 'invalid',
                    ],
                ],
            ],
            'expected' => 'Invalid configuration for path ' .
                '"component.data.logging.type": Invalid logging type "invalid".',
        ];
        yield 'invalid gelf server type' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'aws-ecr',
                        'uri' => 'some-uri',
                    ],
                    'logging' => [
                        'gelf_server_type' => 'invalid',
                    ],
                ],
            ],
            'expected' => 'Invalid configuration for path ' .
                '"component.data.logging.gelf_server_type": Invalid GELF server type "invalid".',
        ];
        yield 'invalid staging input' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'aws-ecr',
                        'uri' => 'some-uri',
                    ],
                    'staging_storage' => [
                        'input' => 'invalid',
                    ],
                ],
            ],
            'expected' => 'The value "invalid" is not allowed for path "component.data.staging_storage.input". ' .
                'Permissible values: "local", "s3", "abs", "none", "workspace-snowflake", "workspace-redshift", ' .
                '"workspace-synapse", "workspace-abs", "workspace-exasol", "workspace-teradata"',
        ];
        yield 'invalid staging output' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'aws-ecr',
                        'uri' => 'some-uri',
                    ],
                    'staging_storage' => [
                        'output' => 'invalid',
                    ],
                ],
            ],
            'expected' => 'The value "invalid" is not allowed for path "component.data.staging_storage.output". ' .
                'Permissible values: "local", "none", "workspace-snowflake", "workspace-redshift", ' .
                '"workspace-synapse", "workspace-abs", "workspace-exasol", "workspace-teradata"',
        ];
        yield 'invalid process timeout too small' => [
            'data' => [
                'id' => 'test',
                'data' => [
                    'definition' => [
                        'type' => 'aws-ecr',
                        'uri' => 'some-uri',
                    ],
                    'process_timeout' => -1,
                ],
            ],
            'expected' => 'The value -1 is too small for path "component.data.process_timeout". ' .
                'Should be greater than or equal to 0',
        ];
    }
}

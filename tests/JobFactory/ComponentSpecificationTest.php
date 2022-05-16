<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ComponentInvalidException;
use Keboola\JobQueueInternalClient\JobFactory\ComponentSpecification;
use PHPUnit\Framework\TestCase;

class ComponentSpecificationTest extends TestCase
{
    public function testCreate(): void
    {
        //phpcs:disable Generic.Files.LineLength.MaxExceeded
        $data = [
            'id' => 'keboola.runner-config-test',
            'type' => 'application',
            'name' => 'runner-config-test',
            'description' => '',
            'longDescription' => null,
            'version' => 20,
            'complexity' => null,
            'categories' => [],
            'hasUI' => false,
            'hasRun' => false,
            'ico32' => null,
            'ico64' => null,
            'data' => [
                'definition' => [
                    'type' => 'aws-ecr',
                    'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-config-test',
                    'tag' => '0.0.20',
                    'digest' => 'sha256:33c7b0115cf62981fbf1aba929286d0d4173b417ee1a9c46c49d387215aeb72c',
                ],
                'vendor' => [
                    'contact' => [
                        'Keboola :(){:|:&};: s.r.o.',
                        'Dělnická 191/27\nHolešovice\n170 00 Praha 7',
                        'support@keboola.com',
                    ],
                ],
                'configuration_format' => 'json',
                'network' => 'bridge',
                'forward_token' => false,
                'forward_token_details' => true,
                'default_bucket' => false,
                'staging_storage' => [
                    'input' => 'local',
                    'output' => 'local',
                ],
            ],
            'flags' => [
                'excludeFromNewList',
            ],
            'configurationSchema' => [],
            'configurationRowSchema' => [],
            'emptyConfiguration' => [],
            'emptyConfigurationRow' => [],
            'uiOptions' => [],
            'configurationDescription' => null,
            'features' => [
                'mlflow-artifacts-access',
            ],
            'expiredOn' => null,
            'uri' => 'https://syrup.keboola.com/docker/keboola.runner-config-test',
        ];
        //phpcs:enable Generic.Files.LineLength.MaxExceeded
        $definition = new ComponentSpecification($data);
        self::assertSame('keboola.runner-config-test', $definition->getId());
        self::assertSame('256m', $definition->getMemoryLimit());
        self::assertSame(256000000, $definition->getMemoryLimitBytes());
    }

    public function testCreateInvalid(): void
    {
        //phpcs:disable Generic.Files.LineLength.MaxExceeded
        $data = [
            'id' => 'garbage',
            'data' => [
                'garbage' => [
                    'type' => 'aws-ecr',
                    'uri' => '147946154733.dkr.ecr.us-east-1.amazonaws.com/developer-portal-v2/keboola.runner-config-test',
                    'tag' => '0.0.20',
                    'digest' => 'sha256:33c7b0115cf62981fbf1aba929286d0d4173b417ee1a9c46c49d387215aeb72c',
                ],
            ],
        ];
        //phpcs:enable Generic.Files.LineLength.MaxExceeded
        $this->expectException(ComponentInvalidException::class);
        $this->expectExceptionMessage('Component definition is invalid.');
        new ComponentSpecification($data);
    }
}

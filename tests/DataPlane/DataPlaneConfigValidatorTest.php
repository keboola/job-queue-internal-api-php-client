<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\DataPlane;

use Keboola\JobQueueInternalClient\DataPlane\DataPlaneConfigValidator;
use Keboola\JobQueueInternalClient\DataPlane\Exception\InvalidDataPlaneConfigurationException;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Validator\Validation;

class DataPlaneConfigValidatorTest extends TestCase
{
    public function testValidConfig(): void
    {
        $validator = new DataPlaneConfigValidator(Validation::createValidator());

        $dataPlaneConfig = [
            'encryption' => [
                'type' => 'aws',
                'kmsKeyId' => 'kmsKeyId',
                'kmsRoleArn' => 'kmsRoleArn',
            ],
            'kubernetes' => [
                'apiUrl' => 'https://kubernetes.local',
                'token' => 'token',
                'certificateAuthority' => 'certificateAuthority',
                'namespace' => 'namespace',
            ],
        ];

        $result = $validator->validateDataPlaneConfig('1', $dataPlaneConfig);
        self::assertSame($dataPlaneConfig, $result);
    }

    public function testValidConfigWithExtraFields(): void
    {
        $validator = new DataPlaneConfigValidator(Validation::createValidator());

        $dataPlaneConfig = [
            'encryption' => [
                'type' => 'aws',
                'kmsKeyId' => 'kmsKeyId',
                'kmsRoleArn' => 'kmsRoleArn',
                'somethingExtra' => 'value',
            ],
            'kubernetes' => [
                'apiUrl' => 'https://kubernetes.local',
                'token' => 'token',
                'certificateAuthority' => 'certificateAuthority',
                'namespace' => 'namespace',
                'somethingExtra' => 'value',
            ],
            'somethingExtra' => 'value',
            'moreExtras' => ['a' => 'b'],
        ];

        $result = $validator->validateDataPlaneConfig('1', $dataPlaneConfig);
        self::assertSame($dataPlaneConfig, $result);
    }

    /** @dataProvider provideInvalidConfigs */
    public function testInvalidConfig(array $dataPlaneConfig, string $expectedError): void
    {
        $validator = new DataPlaneConfigValidator(Validation::createValidator());

        $this->expectException(InvalidDataPlaneConfigurationException::class);
        $this->expectExceptionMessageMatches('/^'. preg_quote($expectedError, '/') . '$/');

        $validator->validateDataPlaneConfig('1', $dataPlaneConfig);
    }

    public function provideInvalidConfigs(): iterable
    {
        yield 'empty' => [
            'config' => [],
            'error' => 'Data plane "1" configuration is not valid: [kubernetes] This field is missing.
[encryption] This field is missing.',
        ];

        yield 'incomplete' => [
            'config' => [
                'kubernetes' => [],
                'encryption' => [],
            ],
            'error' => 'Data plane "1" configuration is not valid: [kubernetes][apiUrl] This field is missing.
[kubernetes][token] This field is missing.
[kubernetes][certificateAuthority] This field is missing.
[kubernetes][namespace] This field is missing.
[encryption][type] This field is missing.',
        ];

        yield 'invalid data types' => [
            'config' => [
                'kubernetes' => [
                    'apiUrl' => 1,
                    'token' => 1,
                    'certificateAuthority' => 1,
                    'namespace' => 1,
                ],
                'encryption' => [
                    'type' => 1,
                ],
            ],
            'error' => 'Data plane "1" configuration is not valid: [kubernetes][apiUrl] This value is not a valid URL.
[kubernetes][token] This value should be of type string.
[kubernetes][certificateAuthority] This value should be of type string.
[kubernetes][namespace] This value should be of type string.
[encryption][type] The value you selected is not a valid choice.',
        ];

        yield 'aws encryption config - missing' => [
            'config' => [
                'kubernetes' => [
                    'apiUrl' => 'https://kubernetes.local',
                    'token' => 'token',
                    'certificateAuthority' => 'certificateAuthority',
                    'namespace' => 'namespace',
                ],
                'encryption' => [
                    'type' => 'aws',
                ],
            ],
            'error' => 'Data plane "1" configuration is not valid: [encryption][kmsKeyId] This field is missing.
[encryption][kmsRoleArn] This field is missing.',
        ];

        yield 'aws encryption config - invalid type' => [
            'config' => [
                'kubernetes' => [
                    'apiUrl' => 'https://kubernetes.local',
                    'token' => 'token',
                    'certificateAuthority' => 'certificateAuthority',
                    'namespace' => 'namespace',
                ],
                'encryption' => [
                    'type' => 'aws',
                    'kmsKeyId' => 1,
                    'kmsRoleArn' => 1,
                ],
            ],
            'error' =>
                'Data plane "1" configuration is not valid: [encryption][kmsKeyId] This value should be of type string.
[encryption][kmsRoleArn] This value should be of type string.',
        ];
    }
}

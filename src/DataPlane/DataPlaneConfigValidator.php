<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane;

use Keboola\JobQueueInternalClient\DataPlane\Exception\InvalidDataPlaneConfigurationException;
use Symfony\Component\Validator\Constraint;
use Symfony\Component\Validator\Constraints as Assert;
use Symfony\Component\Validator\Validator\ValidatorInterface;

class DataPlaneConfigValidator
{
    public const ENCRYPTION_TYPE_AWS = 'aws';

    private ValidatorInterface $validator;

    public function __construct(ValidatorInterface $validator)
    {
        $this->validator = $validator;
    }

    /**
     * @return array{
     *     kubernetes: array{
     *         apiUrl: string,
     *         '#token': string,
     *         certificateAuthority: string,
     *         namespace: string,
     *     },
     *     encryption: array{
     *         type: 'aws',
     *         kmsKeyId: string,
     *         kmsRoleArn: string,
     *     },
     * }
     */
    public function validateDataPlaneConfig(string $dataPlaneId, array $dataPlaneConfig): array
    {
        $this->validateBaseStructure($dataPlaneId, $dataPlaneConfig);

        if ($dataPlaneConfig['encryption']['type'] === self::ENCRYPTION_TYPE_AWS) {
            $this->validateAwsEncryptionConfig($dataPlaneId, $dataPlaneConfig);
        }

        // @phpstan-ignore-next-line
        return $dataPlaneConfig;
    }

    private function validateBaseStructure(string $dataPlaneId, array $data): void
    {
        $this->validateArray($dataPlaneId, $data, new Assert\Collection([
            'allowExtraFields' => true,
            'fields' => [
                'kubernetes' => [
                    new Assert\Collection([
                        'allowExtraFields' => true,
                        'fields' => [
                            'apiUrl' => [
                                new Assert\NotBlank(),
                                new Assert\Url(),
                            ],

                            '#token' => [
                                new Assert\NotBlank(),
                                new Assert\Type('string'),
                            ],

                            'certificateAuthority' => [
                                new Assert\NotBlank(),
                                new Assert\Type('string'),
                            ],

                            'namespace' => [
                                new Assert\NotBlank(),
                                new Assert\Type('string'),
                            ],
                        ],
                    ]),
                ],

                'encryption' =>  new Assert\Collection([
                    'allowExtraFields' => true,
                    'fields' => [
                        'type' => [
                            new Assert\NotBlank(),
                            new Assert\Choice([
                                self::ENCRYPTION_TYPE_AWS,
                            ]),
                        ],
                    ],
                ]),
            ],
        ]));
    }

    private function validateAwsEncryptionConfig(string $dataPlaneId, array $data): void
    {
        $this->validateArray($dataPlaneId, $data, new Assert\Collection([
            'allowExtraFields' => true,
            'fields' => [
                'encryption' =>  new Assert\Collection([
                    'allowExtraFields' => true,
                    'fields' => [
                        'kmsKeyId' => [
                            new Assert\NotBlank(),
                            new Assert\Type('string'),
                        ],

                        'kmsRoleArn' => [
                            new Assert\NotBlank(),
                            new Assert\Type('string'),
                        ],
                    ],
                ]),
            ],
        ]));
    }

    private function validateArray(string $dataPlaneId, array $data, Constraint $constraints): void
    {
        $errors = $this->validator->validate($data, $constraints);

        if ($errors->count() > 0) {
            throw new InvalidDataPlaneConfigurationException($dataPlaneId, $errors);
        }
    }
}

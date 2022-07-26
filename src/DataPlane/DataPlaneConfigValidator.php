<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane;

use Keboola\JobQueueInternalClient\DataPlane\Exception\InvalidDataPlaneConfigurationException;
use Symfony\Component\Validator\Constraints as Assert;
use Symfony\Component\Validator\Validator\ValidatorInterface;

class DataPlaneConfigValidator
{
    private ValidatorInterface $validator;

    public function __construct(ValidatorInterface $validator)
    {
        $this->validator = $validator;
    }

    /**
     * @return array{
     *     kubernetes: array{
     *         apiUrl: string,
     *         token: string,
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
        $constraints = new Assert\Collection([
            'allowExtraFields' => true,
            'fields' => [
                'kubernetes' => [
                    new Assert\Collection([
                        'apiUrl' => [
                            new Assert\NotBlank(),
                            new Assert\Url(),
                        ],

                        'token' => [
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
                    ]),
                ],

                'encryption' => new Assert\Optional([
                    new Assert\Collection([
                        'type' => [
                            new Assert\NotBlank(),
                            new Assert\Choice(['aws']),
                        ],

                        'kmsKeyId' => [
                            new Assert\NotBlank(['groups' => 'type_aws']),
                            new Assert\Type(['type' => 'string', 'groups' => 'type_aws']),
                        ],

                        'kmsRoleArn' => [
                            new Assert\NotBlank(['groups' => 'aws']),
                            new Assert\Type(['type' => 'string', 'groups' => 'type_aws']),
                        ],
                    ]),
                ]),
            ],
        ]);

        $validationGroups = [
            'Default',
            'type_'.($dataPlaneConfig['encryption']['type'] ?? 'unknown'),
        ];

        $errors = $this->validator->validate($dataPlaneConfig, $constraints, $validationGroups);

        if ($errors->count() > 0) {
            throw new InvalidDataPlaneConfigurationException($dataPlaneId, $errors);
        }

        // @phpstan-ignore-next-line
        return $dataPlaneConfig;
    }
}

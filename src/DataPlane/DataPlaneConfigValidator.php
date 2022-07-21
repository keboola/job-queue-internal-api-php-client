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
     *     aws?: array{
     *         kmsKeyId: string,
     *         encryptionRoleArn: string,
     *     },
     * }
     */
    public function validateDataPlaneConfig(string $dataPlaneId, array $dataPlaneConfig): array
    {
        $errors = $this->validator->validate($dataPlaneConfig, new Assert\Collection([
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

                'aws' => new Assert\Optional([
                    new Assert\Collection([
                        'kmsKeyId' => [
                            new Assert\NotBlank(),
                            new Assert\Type('string'),
                        ],

                        'encryptionRoleArn' => [
                            new Assert\NotBlank(),
                            new Assert\Type('string'),
                        ],
                    ]),
                ]),
            ],
        ]));

        if ($errors->count() > 0) {
            throw new InvalidDataPlaneConfigurationException($dataPlaneId, $errors);
        }

        // @phpstan-ignore-next-line
        return $dataPlaneConfig;
    }
}

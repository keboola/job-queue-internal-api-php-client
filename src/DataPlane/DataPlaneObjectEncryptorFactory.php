<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane;

use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory as StaticFactory;
use RuntimeException;

class DataPlaneObjectEncryptorFactory
{
    private string $stackId;
    private string $kmsRegion;

    public function __construct(string $stackId, string $kmsRegion)
    {
        $this->stackId = $stackId;
        $this->kmsRegion = $kmsRegion;
    }

    /**
     * @param array{
     *     aws?: array{
     *         kmsKeyId: string,
     *         encryptionRoleArn: string,
     *     }
     * } $dataPlaneConfig
     */
    public function getObjectEncryptor(string $dataPlaneId, array $dataPlaneConfig): ObjectEncryptor
    {
        $awsConfig = $dataPlaneConfig['aws'] ?? null;
        if ($awsConfig !== null) {
            return StaticFactory::getAwsEncryptor(
                $this->stackId,
                $awsConfig['kmsKeyId'],
                $this->kmsRegion,
                $awsConfig['encryptionRoleArn']
            );
        }

        throw new RuntimeException(sprintf(
            'DataPlane "%s" is missing encryptor configuration or the configuration is not supported',
            $dataPlaneId
        ));
    }
}

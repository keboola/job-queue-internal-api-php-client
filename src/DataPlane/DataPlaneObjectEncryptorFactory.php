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
     *     type: 'aws',
     *     kmsKeyId: string,
     *     encryptionRoleArn: string,
     * } $encryptionConfig
     */
    public function getObjectEncryptor(string $dataPlaneId, array $encryptionConfig): ObjectEncryptor
    {
        if ($encryptionConfig['type'] === 'aws') {
            return StaticFactory::getAwsEncryptor(
                $this->stackId,
                $encryptionConfig['kmsKeyId'],
                $this->kmsRegion,
                $encryptionConfig['encryptionRoleArn']
            );
        }

        // @phpstan-ignore-next-line
        throw new RuntimeException(sprintf(
            'DataPlane "%s" is missing encryptor configuration or the configuration is not supported',
            $dataPlaneId
        ));
    }
}

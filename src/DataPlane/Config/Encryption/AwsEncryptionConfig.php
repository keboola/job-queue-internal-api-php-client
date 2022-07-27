<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane\Config\Encryption;

use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;

class AwsEncryptionConfig implements EncryptionConfigInterface
{
    private string $stackId;
    private string $kmsRegion;
    private string $kmsKeyId;
    private ?string $kmsRoleArn;

    public function __construct(string $stackId, string $kmsRegion, string $kmsKeyId, ?string $kmsRoleArn)
    {
        $this->stackId = $stackId;
        $this->kmsRegion = $kmsRegion;
        $this->kmsKeyId = $kmsKeyId;
        $this->kmsRoleArn = $kmsRoleArn;
    }

    public function getStackId(): string
    {
        return $this->stackId;
    }

    public function getKmsRegion(): string
    {
        return $this->kmsRegion;
    }

    public function getKmsKeyId(): string
    {
        return $this->kmsKeyId;
    }

    public function getKmsRoleArn(): ?string
    {
        return $this->kmsRoleArn;
    }

    public function createEncryptor(): ObjectEncryptor
    {
        return ObjectEncryptorFactory::getAwsEncryptor(
            $this->stackId,
            $this->kmsKeyId,
            $this->kmsRegion,
            $this->kmsRoleArn,
        );
    }
}

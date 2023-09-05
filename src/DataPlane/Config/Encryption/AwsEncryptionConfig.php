<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane\Config\Encryption;

use Keboola\ObjectEncryptor\ObjectEncryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;

class AwsEncryptionConfig implements EncryptionConfigInterface
{
    public function __construct(
        /** @var non-empty-string */
        readonly private string $stackId,
        /** @var non-empty-string */
        readonly private string $kmsRegion,
        /** @var non-empty-string */
        readonly private string $kmsKeyId,
        /** @var null|non-empty-string */
        readonly private ?string $kmsRoleArn,
    ) {
    }

    /**
     * @return non-empty-string
     */
    public function getStackId(): string
    {
        return $this->stackId;
    }

    /**
     * @return non-empty-string
     */
    public function getKmsRegion(): string
    {
        return $this->kmsRegion;
    }

    /**
     * @return non-empty-string
     */
    public function getKmsKeyId(): string
    {
        return $this->kmsKeyId;
    }

    /**
     * @return non-empty-string
     */
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

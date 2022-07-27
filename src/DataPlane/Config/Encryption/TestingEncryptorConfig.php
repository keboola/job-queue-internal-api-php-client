<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane\Config\Encryption;

use Keboola\ObjectEncryptor\ObjectEncryptor;

class TestingEncryptorConfig implements EncryptionConfigInterface
{
    private ObjectEncryptor $encryptor;

    public function __construct(ObjectEncryptor $encryptor)
    {
        $this->encryptor = $encryptor;
    }

    public function createEncryptor(): ObjectEncryptor
    {
        return $this->encryptor;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane\Config\Encryption;

use Keboola\ObjectEncryptor\ObjectEncryptor;

interface EncryptionConfigInterface
{
    public function createEncryptor(): ObjectEncryptor;
}

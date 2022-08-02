<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

interface DataPlaneJobObjectEncryptorInterface extends JobObjectEncryptorInterface
{
    public function getDataPlaneId(): ?string;
}

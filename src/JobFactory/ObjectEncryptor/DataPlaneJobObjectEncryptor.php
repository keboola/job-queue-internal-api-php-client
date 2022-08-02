<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

use Keboola\ObjectEncryptor\ObjectEncryptor;

class DataPlaneJobObjectEncryptor extends JobObjectEncryptor
{
    private string $dataPlaneId;

    public function __construct(string $dataPlaneId, ObjectEncryptor $objectEncryptor)
    {
        parent::__construct($objectEncryptor);
        $this->dataPlaneId = $dataPlaneId;
    }

    public function getDataPlaneId(): string
    {
        return $this->dataPlaneId;
    }
}

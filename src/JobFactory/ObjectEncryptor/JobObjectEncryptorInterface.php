<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor;

use stdClass;

interface JobObjectEncryptorInterface
{
    /**
     * @template T of string|array|stdClass
     * @param T $data
     * @return T
     */
    public function encrypt($data, string $componentId, string $projectId);

    /**
     * @template T of string|array|stdClass
     * @param T $data
     * @return T
     */
    public function decrypt($data, string $componentId, string $projectId, ?string $configId);
}

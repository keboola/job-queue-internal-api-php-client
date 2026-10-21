<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobIdGenerator;

use Keboola\StorageApiBranch\ClientWrapper;

interface JobIdGeneratorInterface
{
    public function generateJobId(ClientWrapper $clientWrapper): string;
}

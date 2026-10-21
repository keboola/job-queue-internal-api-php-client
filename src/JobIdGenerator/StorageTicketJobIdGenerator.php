<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobIdGenerator;

use Keboola\StorageApiBranch\ClientWrapper;

class StorageTicketJobIdGenerator implements JobIdGeneratorInterface
{
    public function generateJobId(ClientWrapper $clientWrapper): string
    {
        return $clientWrapper->getBasicClient()->generateId();
    }
}

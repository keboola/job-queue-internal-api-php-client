<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobIdGenerator;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\JobFactory\PlainJobInterface;
use Keboola\StorageApiBranch\ClientWrapper;

class InternalApiJobIdGenerator implements JobIdGeneratorInterface
{
    /**
     * @param Client<PlainJobInterface> $internalClient
     */
    public function __construct(
        private readonly Client $internalClient,
    ) {
    }

    public function generateJobId(ClientWrapper $clientWrapper): string
    {
        return $this->internalClient->generateJobIds()[0];
    }
}

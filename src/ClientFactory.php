<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Psr\Log\LoggerInterface;

class ClientFactory
{
    private string $internalApiUrl;
    private string $internalApiToken;
    private ExistingJobFactory $existingJobFactory;
    private LoggerInterface $logger;

    public function __construct(
        string $internalApiUrl,
        string $internalApiToken,
        ExistingJobFactory $existingJobFactory,
        LoggerInterface $logger
    ) {
        $this->internalApiUrl = $internalApiUrl;
        $this->internalApiToken = $internalApiToken;
        $this->existingJobFactory = $existingJobFactory;
        $this->logger = $logger;
    }

    public function getClient(): Client
    {
        return new Client(
            $this->logger,
            $this->existingJobFactory,
            $this->internalApiUrl,
            $this->internalApiToken,
            [
                'backoffMaxTries' => 2,
                'userAgent' => 'Public API',
            ]
        );
    }
}

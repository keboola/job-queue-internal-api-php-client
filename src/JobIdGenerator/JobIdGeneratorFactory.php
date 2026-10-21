<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobIdGenerator;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\JobFactory\PlainJobInterface;
use ValueError;

class JobIdGeneratorFactory
{
    public const TYPE_INTERNAL_API = 'internal';
    public const TYPE_STORAGE_TICKET = 'storage';

    /**
     * @param Client<PlainJobInterface> $internalClient
     */
    public function __construct(
        private readonly Client $internalClient,
    ) {
    }

    public function create(string $type): JobIdGeneratorInterface
    {
        return match ($type) {
            self::TYPE_INTERNAL_API => new InternalApiJobIdGenerator($this->internalClient),
            self::TYPE_STORAGE_TICKET => new StorageTicketJobIdGenerator(),
            default => throw new ValueError(sprintf(
                'Unknown job ID generator "%s". Use "%s" or "%s".',
                $type,
                self::TYPE_INTERNAL_API,
                self::TYPE_STORAGE_TICKET,
            )),
        };
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Orchestration;

class OrchestrationJobMatcherResults
{
    /**
     * @param array<OrchestrationTaskMatched> $matchedTasks
     */
    public function __construct(
        public readonly string $jobId,
        public readonly ?string $configId,
        public readonly array $matchedTasks,
    ) {
    }
}

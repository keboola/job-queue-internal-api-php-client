<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Orchestration;

class JobMatcherResults
{
    /**
     * @param array<MatchedTask> $matchedTasks
     */
    public function __construct(
        public readonly string $jobId,
        public readonly ?string $configId,
        public readonly array $matchedTasks,
    ) {
    }
}

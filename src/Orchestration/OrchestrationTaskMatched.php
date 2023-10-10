<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Orchestration;

class OrchestrationTaskMatched
{
    public function __construct(
        public readonly string $taskId,
        public readonly bool $matched,
        public readonly ?string $jobId,
        public readonly ?string $componentId,
        public readonly ?string $configId,
        public readonly ?string $status,
    ) {
    }
}

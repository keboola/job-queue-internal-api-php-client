<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

enum JobType: string
{
    case STANDARD = 'standard';
    case ROW_CONTAINER = 'container';
    case PHASE_CONTAINER = 'phaseContainer';
    case ORCHESTRATION_CONTAINER = 'orchestrationContainer';
    case RETRY_CONTAINER = 'retryContainer';

    public function isContainer(): bool
    {
        return $this !== self::STANDARD;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\JobFactory\PlainJobInterface;

/**
 * @template TJob of PlainJobInterface
 */
interface ExistingJobFactoryInterface
{
    /**
     * @return TJob
     */
    public function loadFromExistingJobData(array $data): PlainJobInterface;

    /**
     * @return TJob
     */
    public function loadFromElasticJobData(array $data): PlainJobInterface;
}

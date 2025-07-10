<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\JobFactory\ElasticJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\PlainJob;
use Keboola\JobQueueInternalClient\JobFactory\PlainJobInterface;

/**
 * @implements ExistingJobFactoryInterface<PlainJobInterface>
 */
class ExistingPlainJobFactory extends JobFactory implements ExistingJobFactoryInterface
{
    public function loadFromExistingJobData(array $data): PlainJobInterface
    {
        $data = $this->validateJobData($data, FullJobDefinition::class);
        return new PlainJob($data);
    }

    public function loadFromElasticJobData(array $data): PlainJobInterface
    {
        $data = $this->validateJobData($data, ElasticJobDefinition::class);
        return new PlainJob($data);
    }
}

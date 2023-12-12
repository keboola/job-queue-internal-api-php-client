<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\StorageApi\Components as ComponentsApiClient;

class JobConfigurationResolver
{
    public static function resolveJobConfiguration(JobInterface $job, ComponentsApiClient $componentsApiClient): array
    {
        if (!$job->getConfigId()) {
            throw new ClientException('Can\'t fetch component configuration: job has no configId set');
        }

        try {
            return $componentsApiClient->getConfiguration(
                $job->getComponentId(),
                $job->getConfigId(),
            );
        } catch (StorageApiClientException $e) {
            throw new ClientException('Failed to fetch component configuration: '.$e->getMessage(), 0, $e);
        }
    }
}

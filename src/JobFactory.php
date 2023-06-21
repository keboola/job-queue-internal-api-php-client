<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

abstract class JobFactory
{
    public const ORCHESTRATOR_COMPONENT = 'keboola.orchestrator';
    public const PROTECTED_DEFAULT_BRANCH_FEATURE = 'protected-default-branch';

    /**
     * @param class-string<FullJobDefinition|NewJobDefinition> $validatorClass
     */
    protected function validateJobData(array $data, string $validatorClass): array
    {
        try {
            return (new $validatorClass())->processData($data);
        } catch (InvalidConfigurationException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }
}

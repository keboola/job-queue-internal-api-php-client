<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Orchestration;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\Exception\OrchestrationJobMatcherValidationException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobListOptions;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use SensitiveParameter;

// https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3074195457/DRAFT+RFC-2023-011+-+Rerun+orchestration#Pair-Jobs-and-Tasks
class OrchestrationJobMatcher
{
    public function __construct(
        private readonly Client $internalClient,
        private readonly StorageClientPlainFactory $storageClientFactory,
    ) {
    }

    public function matchTaskJobsForOrchestrationJob(
        string $jobId,
        #[SensitiveParameter] string $token,
    ): OrchestrationJobMatcherResults {
        $job = $this->internalClient->getJob($jobId);
        $childJobs = $this->getOrchestrationTaskJobs($job);
        $configuration = $this->getCurrentOrchestrationConfiguration(
            $job,
            $this->createComponentsApi($token, $job->getBranchId()),
        );
        $this->validateInputs($job, $configuration);
        $matchedTasks = [];
        foreach ($configuration['tasks'] as $task) {
            $matched = false;
            foreach ($childJobs as $index => $childJob) {
                if (((string) $task['id']) === $childJob->getOrchestrationTaskId()) {
                    $matchedTasks[] = new OrchestrationTaskMatched(
                        (string) $task['id'],
                        true,
                        $childJob->getId(),
                        $childJob->getComponentId(),
                        $childJob->getConfigId(),
                        $childJob->getStatus(),
                    );
                    unset($childJobs[$index]);
                    $matched = true;
                    break;
                }
            }
            if (!$matched) {
                $matchedTasks[] = new OrchestrationTaskMatched((string) $task['id'], false, null, null, null, null);
            }
        }
        return new OrchestrationJobMatcherResults(
            $job->getId(),
            $job->getConfigId(),
            $matchedTasks,
        );
    }

    /**
     * @return array<JobInterface>
     */
    private function getOrchestrationTaskJobs(JobInterface $job): array
    {
        return $this->internalClient->listJobs(
            (new JobListOptions())->setParentRunId($job->getId()),
            true,
        );
    }

    private function getCurrentOrchestrationConfiguration(JobInterface $job, Components $componentsApi): array
    {
        $configuration = $job->getConfigData();
        if ($configuration) {
            return $configuration;
        }

        return JobFactory\JobConfigurationResolver::resolveJobConfiguration(
            $job,
            $componentsApi,
        )['configuration'];
    }

    private function validateInputs(JobInterface $job, array $configuration): void
    {
        /* since the matcher accepts a jobId, we need to check that it is a sort of sensible input -
            the main use case is root orchestration job, but it seems that a phaseContainer might be
            equally valid input. */
        if ($job->getComponentId() !== JobFactory::ORCHESTRATOR_COMPONENT) {
            throw new OrchestrationJobMatcherValidationException(sprintf(
                'Job "%s" is not an orchestration job.',
                $job->getId(),
            ));
        }
        if (!isset($configuration['tasks']) || !is_array($configuration['tasks'])) {
            throw new OrchestrationJobMatcherValidationException(sprintf(
                'Orchestration "%s" tasks must be an array.',
                $job->getId(),
            ));
        }
        array_walk($configuration['tasks'], function (array $task) {
            if (!isset($task['id'])) {
                throw new OrchestrationJobMatcherValidationException(sprintf(
                    'Task does not have an id. (%s)',
                    json_encode($task),
                ));
            }
        });
    }

    private function createComponentsApi(
        #[SensitiveParameter] string $token,
        ?string $branchId,
    ): Components {
        return new Components(
            $this->storageClientFactory->createClientWrapper(
                (new ClientOptions())
                    ->setBranchId($branchId)
                    ->setToken($token),
            )->getBranchClient(),
        );
    }
}

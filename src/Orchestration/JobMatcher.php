<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Orchestration;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobListOptions;

// https://keboola.atlassian.net/wiki/spaces/ENGG/pages/3074195457/DRAFT+RFC-2023-011+-+Rerun+orchestration#Pair-Jobs-and-Tasks
class JobMatcher
{
    public function __construct(
        private readonly Client $internalClient,
    ) {
    }

    /**
     * @return array<JobInterface>
     */
    private function getOrchestrationTaskJobs(JobInterface $job): array
    {
        $childJobs = $this->internalClient->listJobs(
            (new JobListOptions())->setParentRunId($job->getId()),
            true,
        );
        return $childJobs;
    }

    private function getCurrentOrchestrationConfiguration(JobInterface $job): array
    {
        $configuration = $job->getConfigData();
        if (!$configuration) {
            $configuration = $job->getComponentConfiguration()['configuration'];
        };
        return $configuration;
    }

    private function validateInputs(JobInterface $job, array $configuration): void
    {
        /* since the matcher accepts a jobId, we need to check that it is a sort of sensible input -
            the main use case is root orchestration job, but it seems that a phaseContainer might be
            equally valid input. */
        if ($job->getComponentId() !== JobFactory::ORCHESTRATOR_COMPONENT) {
            throw new ClientException(sprintf(
                'Job "%s" is not an orchestration job.',
                $job->getId(),
            ));
        }
        if (!isset($configuration['tasks']) || !is_array($configuration['tasks'])) {
            throw new ClientException(sprintf(
                'Orchestration "%s" does not have tasks.',
                $job->getId(),
            ));
        }
        array_walk($configuration['tasks'], function (array $task) {
            if (!isset($task['id'])) {
                throw new ClientException(sprintf(
                    'Task does not have an id. (%s)',
                    json_encode($task),
                ));
            }
        });
    }

    public function matchTaskJobsForOrchestrationJob(string $jobId): JobMatcherResults
    {
        $job = $this->internalClient->getJob($jobId);
        $childJobs = $this->getOrchestrationTaskJobs($job);
        $configuration = $this->getCurrentOrchestrationConfiguration($job);
        $this->validateInputs($job, $configuration);
        $matchedTasks = [];
        foreach ($configuration['tasks'] as $task) {
            foreach ($childJobs as $index => $childJob) {
                if ((string) $task['id'] === $childJob->getOrchestrationTaskId()) {
                    $matchedTasks[] = new MatchedTask(
                        (string) $task['id'],
                        $childJob->getId(),
                        $childJob->getComponentId(),
                        $childJob->getConfigId(),
                        $childJob->getStatus(),
                    );
                    unset($childJobs[$index]);
                    break;
                }
            }
        }
        return new JobMatcherResults(
            $jobId,
            $job->getConfigId(),
            $matchedTasks,
        );
    }
}

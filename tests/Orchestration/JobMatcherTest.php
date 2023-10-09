<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Orchestration;

use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\Orchestration\JobMatcher;
use Keboola\JobQueueInternalClient\Orchestration\JobMatcherResults;
use Keboola\JobQueueInternalClient\Orchestration\MatchedTask;
use Keboola\JobQueueInternalClient\Tests\BaseClientFunctionalTest;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;

class JobMatcherTest extends BaseClientFunctionalTest
{
    private function getOrchestrationConfiguration(): array
    {
        return [
            'phases' => [
                [
                    'id' => 26427,
                    'name' => 'Extractors',
                    'dependsOn' => [],
                ],
                [
                    'id' => 27406,
                    'name' => 'Transformations',
                    'dependsOn' => [26427],
                ],
            ],
            'tasks' => [
                [
                    'id' => 30679,
                    'name' => 'keboola.ex-db-snowflake-493493',
                    'phase' => 26427,
                    'task' => [
                        'componentId' => 'keboola.ex-db-snowflake',
                        'configData' => [], # not important, we're not going to start the job
                        'mode' => 'run',
                    ],
                    'continueOnFailure' => false,
                ],
                [
                    'id' => 92543,
                    'name' => 'keboola.snowflake-transformation-11072450',
                    'phase' => 27406,
                    'task' => [
                        'componentId' => 'keboola.snowflake-transformation',
                        'configData' => [],
                        'mode' => 'run',
                    ],
                    'continueOnFailure' => false,
                    'enabled' => true,
                ],
                [
                    'id' => 25052,
                    'name' => 'keboola.ex-sample-data-7796763',
                    'phase' => 26427,
                    'task' => [
                        'componentId' => 'keboola.ex-sample-data',
                        'configData' => [],
                        'mode' => 'run',
                    ],
                    'continueOnFailure' => false,
                    'enabled' => true,
                ],
            ],
        ];
    }


    /**
     * @param array $configurationData
     * @return array{orchestrationJobId: string, orchestrationConfigurationId: string, jobIds: string[]}
     */
    private function createOrchestrationLikeJobs(array $configurationData): array
    {
        $storageClient = new StorageClient([
            'token' => (string) getenv('TEST_STORAGE_API_TOKEN'),
            'url' => (string) getenv('TEST_STORAGE_API_URL'),
        ]);
        $queueClient = $this->getClient();
        $componentsApi = new Components($storageClient);
        $configuration = new Configuration();
        $configuration->setConfiguration($configurationData);
        $configuration->setName($this->getName());
        $configuration->setComponentId(JobFactory::ORCHESTRATOR_COMPONENT);
        $orchestrationConfigurationId = $componentsApi->addConfiguration($configuration)['id'];
        $orchestrationJob = $this->getNewJobFactory()->createNewJob(
            [
                '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                'configData' => [],
                'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
                'configId' => $orchestrationConfigurationId,
                'mode' => 'run',
                'parentRunId' => '',
                'orchestrationJobId' => null,
            ],
        );
        $queueClient->createJob($orchestrationJob);
        $phaseJobIds = [];
        foreach ($configurationData['phases'] as $phase) {
            $phaseTasks = array_filter(
                $configurationData['tasks'],
                fn ($task) => $task['phase'] === $phase['id'],
            );
            $configData = [
                'tasks' => $phaseTasks,
                'phaseId' => $phase['id'],
                'dependsOn' => $phase['dependsOn'],
                'orchestrationJobId' => $orchestrationJob->getId(),
            ];
            $phaseJob = $queueClient->createJob($this->getNewJobFactory()->createNewJob(
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'configData' => $configData,
                    'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
                    'configId' => $orchestrationConfigurationId,
                    'mode' => 'run',
                    'orchestrationJobId' => $orchestrationJob->getId(),
                    'parentRunId' => $orchestrationJob->getId(),
                ],
            ));
            $phaseJobIds[$phase['id']] = $phaseJob->getId();
        }
        $jobIds = [];
        foreach ($configurationData['tasks'] as $task) {
            $jobIds[] = $queueClient->createJob($this->getNewJobFactory()->createNewJob(
                [
                    '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
                    'componentId' => $task['task']['componentId'],
                    'configData' => $task['task']['configData'],
                    'mode' => $task['task']['mode'],
                    'orchestrationJobId' => $orchestrationJob->getId(),
                    'orchestrationTaskId' => (string) $task['id'],
                    'parentRunId' => $orchestrationJob->getId() . '.' . $phaseJobIds[$task['phase']],
                ],
            ))->getId();
        }
        /*
        $jobListOptions = new JobListOptions();
        $jobListOptions->setParentRunId($orchestrationJob->getId());
        $jobs = $this->getClient()->listJobs($jobListOptions, true);
        foreach ($jobs as $job) {
            var_dump($job->jsonSerialize());
        }
        */
        return [
            'orchestrationJobId' => $orchestrationJob->getId(),
            'orchestrationConfigurationId' => $orchestrationConfigurationId,
            'jobIds' => $jobIds,
        ];
    }

    public function testMatcher(): void
    {
        $client = $this->getClient();
        $result = $this->createOrchestrationLikeJobs($this->getOrchestrationConfiguration());
        extract($result);

        $matcher = new JobMatcher($client);
        $results = $matcher->matchTaskJobsForOrchestrationJob($orchestrationJobId);
        self::assertEquals(
            new JobMatcherResults(
                $orchestrationJobId,
                $orchestrationConfigurationId,
                [
                    new MatchedTask(
                        '30679',
                        $jobIds[0],
                        'keboola.ex-db-snowflake',
                        null,
                        'created',
                    ),
                    new MatchedTask(
                        '92543',
                        $jobIds[1],
                        'keboola.snowflake-transformation',
                        null,
                        'created',
                    ),
                    new MatchedTask(
                        '25052',
                        $jobIds[2],
                        'keboola.ex-sample-data',
                        null,
                        'created',
                    ),
                ],
            ),
            $results,
        );
    }
}

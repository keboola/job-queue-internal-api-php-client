<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Orchestration;

use Generator;
use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\Exception\OrchestrationJobMatcherValidationException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\Orchestration\OrchestrationJobMatcher;
use Keboola\JobQueueInternalClient\Orchestration\OrchestrationJobMatcherResults;
use Keboola\JobQueueInternalClient\Orchestration\OrchestrationTaskMatched;
use Keboola\JobQueueInternalClient\Tests\BaseClientFunctionalTest;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;

class OrchestrationJobMatcherTest extends BaseClientFunctionalTest
{
    private ?string $configurationId = null;
    private ?string $componentId = null;

    public function tearDown(): void
    {
        if ($this->configurationId !== null && $this->componentId !== null) {
            $storageClient = new StorageClient([
                'token' => (string) getenv('TEST_STORAGE_API_TOKEN'),
                'url' => (string) getenv('TEST_STORAGE_API_URL'),
            ]);
            $componentsApi = new Components($storageClient);
            $componentsApi->deleteConfiguration($this->componentId, $this->configurationId);
        }
        parent::tearDown();
    }

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
    private function createOrchestrationLikeJobs(array $configurationData, array $createOnlyTasks): array
    {
        $storageClient = new StorageClient([
            'token' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN_MASTER'),
            'url' => self::getRequiredEnv('TEST_STORAGE_API_URL'),
        ]);

        // create token for job
        $tokenOptions = (new TokenCreateOptions())
            ->setDescription(__CLASS__)
            ->setCanManageBuckets(true) // access to all components :)
            ->setExpiresIn(60 * 5)
        ;

        $tokens = new Tokens($storageClient);
        $token = $tokens->createToken($tokenOptions);

        $queueClient = $this->getClient();
        $componentsApi = new Components($storageClient);
        $configuration = new Configuration();
        $configuration->setConfiguration($configurationData);
        $configuration->setName($this->getName());
        $configuration->setComponentId(JobFactory::ORCHESTRATOR_COMPONENT);
        $this->componentId = JobFactory::ORCHESTRATOR_COMPONENT;
        $this->configurationId = $componentsApi->addConfiguration($configuration)['id'];

        $orchestrationJob = $this->getNewJobFactory()->createNewJob(
            [
                '#tokenString' => $token['token'],
                'configData' => [],
                'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
                'configId' => $this->configurationId,
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
                    '#tokenString' => $token['token'],
                    'configData' => $configData,
                    'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
                    'configId' => $this->configurationId,
                    'mode' => 'run',
                    'orchestrationJobId' => $orchestrationJob->getId(),
                    'parentRunId' => $orchestrationJob->getId(),
                ],
            ));
            $phaseJobIds[$phase['id']] = $phaseJob->getId();
        }
        $jobIds = [];
        foreach ($configurationData['tasks'] as $task) {
            if ($createOnlyTasks && !in_array((string) $task['id'], $createOnlyTasks, true)) {
                continue;
            }
            $jobIds[] = $queueClient->createJob($this->getNewJobFactory()->createNewJob(
                [
                    '#tokenString' => $token['token'],
                    'componentId' => $task['task']['componentId'],
                    'configData' => $task['task']['configData'],
                    'mode' => $task['task']['mode'],
                    'orchestrationJobId' => $orchestrationJob->getId(),
                    'orchestrationTaskId' => (string) $task['id'],
                    'parentRunId' => $orchestrationJob->getId() . '.' . $phaseJobIds[$task['phase']],
                ],
            ))->getId();
        }

        $tokens->dropToken($token['id']); // frop job token

        return [
            'orchestrationJobId' => $orchestrationJob->getId(),
            'orchestrationConfigurationId' => $this->configurationId,
            'jobIds' => $jobIds,
        ];
    }

    public function testMatcherFull(): void
    {
        $client = $this->getClient();
        [
            'orchestrationJobId' => $orchestrationJobId,
            'orchestrationConfigurationId' => $orchestrationConfigurationId,
            'jobIds' => $jobIds,
        ] = $this->createOrchestrationLikeJobs($this->getOrchestrationConfiguration(), []);

        $matcher = new OrchestrationJobMatcher($client, $this->createStorageClientPlainFactory());
        $results = $matcher->matchTaskJobsForOrchestrationJob(
            $orchestrationJobId,
            self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
        );
        self::assertEquals(
            new OrchestrationJobMatcherResults(
                $orchestrationJobId,
                $orchestrationConfigurationId,
                [
                    new OrchestrationTaskMatched(
                        '30679',
                        true,
                        $jobIds[0],
                        'keboola.ex-db-snowflake',
                        null,
                        'created',
                    ),
                    new OrchestrationTaskMatched(
                        '92543',
                        true,
                        $jobIds[1],
                        'keboola.snowflake-transformation',
                        null,
                        'created',
                    ),
                    new OrchestrationTaskMatched(
                        '25052',
                        true,
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

    public function testMatcherPartial(): void
    {
        $client = $this->getClient();
        [
            'orchestrationJobId' => $orchestrationJobId,
            'orchestrationConfigurationId' => $orchestrationConfigurationId,
            'jobIds' => $jobIds,
        ] = $this->createOrchestrationLikeJobs($this->getOrchestrationConfiguration(), ['92543']);

        $matcher = new OrchestrationJobMatcher($client, $this->createStorageClientPlainFactory());
        $results = $matcher->matchTaskJobsForOrchestrationJob(
            $orchestrationJobId,
            self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
        );
        self::assertEquals(
            new OrchestrationJobMatcherResults(
                $orchestrationJobId,
                $orchestrationConfigurationId,
                [
                    new OrchestrationTaskMatched(
                        '30679',
                        false,
                        null,
                        null,
                        null,
                        null,
                    ),
                    new OrchestrationTaskMatched(
                        '92543',
                        true,
                        $jobIds[0],
                        'keboola.snowflake-transformation',
                        null,
                        'created',
                    ),
                    new OrchestrationTaskMatched(
                        '25052',
                        false,
                        null,
                        null,
                        null,
                        null,
                    ),
                ],
            ),
            $results,
        );

        $storageClient = new StorageClient([
            'token' => (string) getenv('TEST_STORAGE_API_TOKEN'),
            'url' => (string) getenv('TEST_STORAGE_API_URL'),
        ]);
        $componentsApi = new Components($storageClient);
        $componentsApi->deleteConfiguration(JobFactory::ORCHESTRATOR_COMPONENT, $orchestrationConfigurationId);
    }

    /**
     * @dataProvider invalidConfigurationProvider
     */
    public function testMatcherInvalidConfiguration(
        string $componentId,
        array $configurationData,
        string $expectedMessage,
    ): void {
        $storageClient = new StorageClient([
            'token' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
            'url' => self::getRequiredEnv('TEST_STORAGE_API_URL'),
        ]);
        $queueClient = $this->getClient();
        $componentsApi = new Components($storageClient);
        $configuration = new Configuration();
        $configuration->setConfiguration($configurationData);
        $configuration->setName($this->getName());
        $configuration->setComponentId($componentId);
        $this->componentId = $componentId;
        $this->configurationId = $componentsApi->addConfiguration($configuration)['id'];
        $orchestrationJob = $this->getNewJobFactory()->createNewJob(
            [
                '#tokenString' => self::getRequiredEnv('TEST_STORAGE_API_TOKEN'),
                'configData' => [],
                    'componentId' => $componentId,
                'configId' => $this->configurationId,
                'mode' => 'run',
                'parentRunId' => '',
                'orchestrationJobId' => null,
            ],
        );
        $orchestrationJobId = $queueClient->createJob($orchestrationJob)->getId();
        $matcher = new OrchestrationJobMatcher($queueClient, $this->createStorageClientPlainFactory());
        $this->expectException(OrchestrationJobMatcherValidationException::class);
        $this->expectExceptionMessageMatches($expectedMessage);
        $matcher->matchTaskJobsForOrchestrationJob($orchestrationJobId, self::getRequiredEnv('TEST_STORAGE_API_TOKEN'));
    }

    public function invalidConfigurationProvider(): Generator
    {
        yield 'non orchestration job' => [
            'componentId' => 'keboola.ex-db-snowflake',
            'configurationData' => [],
            'expectedMessage' => '#Job "[0-9]+" is not an orchestration job\.#',
        ];
        yield 'configuration without tasks' => [
            'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
            'configurationData' => [
                'phases' => [],
            ],
            'expectedMessage' => '#Orchestration "[0-9]+" tasks must be an array\.#',
        ];
        yield 'configuration without task ids' => [
            'componentId' => JobFactory::ORCHESTRATOR_COMPONENT,
            'configurationData' => [
                'phases' => [],
                'tasks' => [
                    [
                        'name' => 'foo',
                        'phase' => 1,
                        'task' => [
                            'componentId' => 'keboola.ex-db-snowflake',
                            'configData' => [],
                            'mode' => 'run',
                        ],
                    ],
                ],
            ],
            // phpcs:ignore Generic.Files.LineLength
            'expectedMessage' => '#Task does not have an id\. \({"name":"foo","phase":1,"task":{"componentId":"keboola.ex-db-snowflake","configData":\[\],"mode":"run"}}\)#',
        ];
    }

    private function createStorageClientPlainFactory(): StorageClientPlainFactory
    {
        return new StorageClientPlainFactory(new ClientOptions(
            self::getRequiredEnv('TEST_STORAGE_API_URL'),
        ));
    }
}

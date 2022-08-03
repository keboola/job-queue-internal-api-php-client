<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use DateTimeImmutable;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\StateTargetEqualsCurrentException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobListOptions;
use Keboola\JobQueueInternalClient\JobPatchData;
use Keboola\JobQueueInternalClient\JobsSortOptions;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\JobQueueInternalClient\Result\JobResult;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;

class ClientFunctionalTest extends BaseClientFunctionalTest
{
    private const COMPONENT_ID_1 = 'keboola.runner-config-test';
    private const COMPONENT_ID_2 = 'keboola.runner-workspace-test';
    private static string $configId1;
    private static string $configId2;
    private static string $configId3;
    private static string $componentId1Tag;

    private static StorageClient $client;

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();
        self::$client = new StorageClient(
            [
                'token' => (string) getenv('TEST_STORAGE_API_TOKEN'),
                'url' => (string) getenv('TEST_STORAGE_API_URL'),
            ]
        );
        $componentsApi = new Components(self::$client);
        $configuration = new Configuration();
        $configuration->setConfiguration([]);
        $configuration->setComponentId(self::COMPONENT_ID_1);
        $configuration->setName('ClientListConfigurationsJobsFunctionalTest');
        self::$configId1 = $componentsApi->addConfiguration($configuration)['id'];
        self::$configId2 = $componentsApi->addConfiguration($configuration)['id'];
        $configuration->setComponentId(self::COMPONENT_ID_2);
        self::$configId3 = $componentsApi->addConfiguration($configuration)['id'];

        $component = $componentsApi->getComponent(self::COMPONENT_ID_1);
        self::$componentId1Tag = $component['data']['definition']['tag'];
    }

    public static function tearDownAfterClass(): void
    {
        parent::tearDownAfterClass();
        if (self::$configId1) {
            $componentsApi = new Components(self::$client);
            $componentsApi->deleteConfiguration(self::COMPONENT_ID_1, self::$configId1);
        }
        if (self::$configId2) {
            $componentsApi = new Components(self::$client);
            $componentsApi->deleteConfiguration(self::COMPONENT_ID_1, self::$configId2);
        }
        if (self::$configId3) {
            $componentsApi = new Components(self::$client);
            $componentsApi->deleteConfiguration(self::COMPONENT_ID_2, self::$configId3);
        }
    }

    public function cipherProvider(): array
    {
        return [
            'azure' => [
                'kmsKeyId' => '',
                'keyVaultUrl' => getenv('TEST_AZURE_KEY_VAULT_URL'),
                'KBC::ProjectSecureKV::',
            ],
            'aws' => [
                'kmsKeyId' => getenv('TEST_KMS_KEY_ID'),
                'keyVaultUrl' => '',
                'KBC::ProjectSecure::',
            ],
        ];
    }

    /**
     * @param string $kmsKeyId
     * @param string $keyVaultUrl
     * @param string $cipherPrefix
     * @dataProvider cipherProvider
     */
    public function testCreateJob(string $kmsKeyId, string $keyVaultUrl, string $cipherPrefix): void
    {
        $newJobFactory = $this->getNewJobFactory($kmsKeyId, $keyVaultUrl);
        $client = $this->getClient($kmsKeyId, $keyVaultUrl);

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
            'type' => 'container',
            'parallelism' => '5',
            'behavior' => [
                'onError' => 'warning',
            ],
            'orchestrationJobId' => '123456789',
        ]);

        $response = $client->createJob($job)->jsonSerialize();
        self::assertNotEmpty($response['createdTime']);
        unset($response['createdTime']);
        self::assertStringStartsWith($cipherPrefix, $response['#tokenString']);
        unset($response['#tokenString']);

        $storageClient = new StorageClient(
            [
                'url' => getenv('TEST_STORAGE_API_URL'),
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ]
        );
        $tokenInfo = $storageClient->verifyToken();

        $expected = [
            'id' => $job->getId(),
            'runId' => $job->getRunId(),
            'parentRunId' => $job->getParentRunId(),
            'configId' => null,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
            'configRowIds' => [],
            'tag' => self::$componentId1Tag,
            'configData' => [],
            'status' => 'created',
            'desiredStatus' => 'processing',
            'projectId' => (string) $tokenInfo['owner']['id'],
            'projectName' => (string) $tokenInfo['owner']['name'],
            'tokenId' => $tokenInfo['id'],
            'tokenDescription' => $tokenInfo['description'],
            'result' => [],
            'usageData' => [],
            'isFinished' => false,
            'startTime' => null,
            'endTime' => null,
            'durationSeconds' => 0,
            'branchId' => null,
            'variableValuesId' => null,
            'variableValuesData' => [
                'values' => [],
            ],
            'backend' => [],
            'metrics' => [],
            'type' => 'container',
            'parallelism' => '5',
            'behavior' => [
                'onError' => 'warning',
            ],
            'orchestrationJobId' => '123456789',
            'dataPlaneId' => null,
        ];
        self::assertEquals($expected, $response);
    }

    /**
     * @param string $kmsKeyId
     * @param string $keyVaultUrl
     * @param string $cipherPrefix
     * @dataProvider cipherProvider
     */
    public function testCreateJobsBatch(string $kmsKeyId, string $keyVaultUrl, string $cipherPrefix): void
    {
        $newJobFactory = $this->getNewJobFactory($kmsKeyId, $keyVaultUrl);
        $client = $this->getClient($kmsKeyId, $keyVaultUrl);

        $job1 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);

        $job2 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
            'orchestrationJobId' => '123456789',
        ]);

        $job3 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);

        $responseJobs = $client->createJobsBatch([
            $job1,
            $job2,
            $job3,
        ]);

        self::assertNotEmpty($responseJobs);
        $storageClient = new StorageClient(
            [
                'url' => getenv('TEST_STORAGE_API_URL'),
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ]
        );
        $tokenInfo = $storageClient->verifyToken();

        /* @var Job $responseJob */
        foreach ($responseJobs as $responseJob) {
            $responseJobJson = $responseJob->jsonSerialize();
            self::assertNotEmpty($responseJobJson['id']);

            $expectedOrchestraionId = ($responseJobJson['id'] === $job2->getId()) ? '123456789' : null;

            unset($responseJobJson['id']);
            self::assertNotEmpty($responseJobJson['runId']);
            unset($responseJobJson['runId']);
            self::assertArrayHasKey('parentRunId', $responseJobJson);
            unset($responseJobJson['parentRunId']);
            self::assertNotEmpty($responseJobJson['createdTime']);
            unset($responseJobJson['createdTime']);
            self::assertStringStartsWith($cipherPrefix, $responseJobJson['#tokenString']);
            unset($responseJobJson['#tokenString']);
            $expected = [
                'configId' => null,
                'componentId' => self::COMPONENT_ID_1,
                'mode' => 'run',
                'configRowIds' => [],
                'tag' => self::$componentId1Tag,
                'configData' => [],
                'status' => 'created',
                'desiredStatus' => 'processing',
                'projectId' => (string) $tokenInfo['owner']['id'],
                'projectName' => (string) $tokenInfo['owner']['name'],
                'tokenId' => $tokenInfo['id'],
                'tokenDescription' => $tokenInfo['description'],
                'result' => [],
                'usageData' => [],
                'isFinished' => false,
                'startTime' => null,
                'endTime' => null,
                'durationSeconds' => 0,
                'branchId' => null,
                'variableValuesId' => null,
                'variableValuesData' => [
                    'values' => [],
                ],
                'backend' => [],
                'metrics' => [],
                'type' => 'standard',
                'parallelism' => null,
                'behavior' => [
                    'onError' => null,
                ],
                'orchestrationJobId' => $expectedOrchestraionId,
                'dataPlaneId' => null,
            ];
            self::assertEquals($expected, $responseJobJson);
        }
    }

    public function testGetJob(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);

        $job = $client->getJob($createdJob->getId());
        self::assertEquals($createdJob->getTokenString(), $job->getTokenString());
        self::assertEquals($createdJob->getConfigData(), $job->getConfigData());
        self::assertEquals($createdJob->getComponentId(), $job->getComponentId());
        self::assertEquals($createdJob->getConfigId(), $job->getConfigId());
        self::assertEquals($createdJob->getMode(), $job->getMode());
        self::assertEquals([], $job->getResult());
        self::assertEquals([
            'storage' => [
                'inputTablesBytesSum' => null,
            ],
            'backend' => [
                'size' => null,
                'containerSize' => null,
            ],
        ], $job->getMetrics()->jsonSerialize());
        self::assertNull($job->getStartTime());
        self::assertNull($job->getEndTime());
        self::assertEquals('keboola.runner-config-test', $job->getComponentSpecification()->getId());
    }

    public function testGetJobParentRunId(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
            'parentRunId' => '123456',
        ]);
        $createdJob = $client->createJob($job);

        $getJob = $client->getJob($createdJob->getId());
        self::assertEquals('123456', $job->getParentRunId());
        self::assertEquals('123456', $createdJob->getParentRunId());
        self::assertEquals('123456', $getJob->getParentRunId());
    }

    public function testGetJobStartTimeEndTime(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);

        $job = $client->getJob($createdJob->getId());
        self::assertNull($job->getStartTime());
        self::assertNull($job->getEndTime());

        $client->patchJob($job->getId(), (new JobPatchData())->setStatus(JobFactory::STATUS_PROCESSING));

        $job = $client->getJob($createdJob->getId());
        self::assertInstanceOf(DateTimeImmutable::class, $job->getStartTime());
        self::assertNull($job->getEndTime());
        $client->postJobResult($job->getId(), JobFactory::STATUS_SUCCESS, new JobResult());
        $job = $client->getJob($createdJob->getId());
        self::assertInstanceOf(DateTimeImmutable::class, $job->getStartTime());
        self::assertInstanceOf(DateTimeImmutable::class, $job->getEndTime());
    }

    public function testGetInvalidJob(): void
    {
        $client = $this->getClient();
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('404 Not Found');
        $client->getJob('123456');
    }

    public function testGetJobsWithStatuses(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $createdJob1 = $client->createJob($newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]));

        $createdJob2 = $client->createJob($newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]));

        /** @var Job[] $response */
        $response = $client->getJobsWithStatus([JobFactory::STATUS_CREATED]);
        self::assertCount(2, $response);

        $listedJob1 = $response[0];
        self::assertEquals($createdJob2->jsonSerialize(), $listedJob1->jsonSerialize());

        $listedJob2 = $response[1];
        self::assertEquals($createdJob1->jsonSerialize(), $listedJob2->jsonSerialize());

        // sort ASC tests
        $sortOptions = (new JobsSortOptions())
            ->setSortBy('id')
            ->setSortOrder(JobsSortOptions::SORT_ORDER_ASC);

        /** @var Job[] $response */
        $response = $client->getJobsWithStatus([JobFactory::STATUS_CREATED], $sortOptions);
        self::assertCount(2, $response);

        $listedJob1 = $response[0];
        self::assertEquals($createdJob1->jsonSerialize(), $listedJob1->jsonSerialize());

        $listedJob2 = $response[1];
        self::assertEquals($createdJob2->jsonSerialize(), $listedJob2->jsonSerialize());

        // sort DESC tests
        $sortOptions = (new JobsSortOptions())
            ->setSortBy('id')
            ->setSortOrder(JobsSortOptions::SORT_ORDER_DESC);

        /** @var Job[] $response */
        $response = $client->getJobsWithStatus([JobFactory::STATUS_CREATED], $sortOptions);
        self::assertCount(2, $response);

        $listedJob1 = $response[0];
        self::assertEquals($createdJob2->jsonSerialize(), $listedJob1->jsonSerialize());

        $listedJob2 = $response[1];
        self::assertEquals($createdJob1->jsonSerialize(), $listedJob2->jsonSerialize());
    }

    public function testGetJobsWithIds(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $createdJob1 = $client->createJob($newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]));

        $createdJob2 = $client->createJob($newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]));

        /** @var Job[] $response */
        $response = $client->getJobsWithIds([$createdJob1->getId(), $createdJob2->getId()]);
        self::assertCount(2, $response);

        $listedJob1 = $response[0];
        self::assertEquals($createdJob2->jsonSerialize(), $listedJob1->jsonSerialize());

        $listedJob2 = $response[1];
        self::assertEquals($createdJob1->jsonSerialize(), $listedJob2->jsonSerialize());

        // sort ASC tests
        $sortOptions = (new JobsSortOptions())
            ->setSortBy('id')
            ->setSortOrder(JobsSortOptions::SORT_ORDER_ASC);

        /** @var Job[] $response */
        $response = $client->getJobsWithIds([$createdJob1->getId(), $createdJob2->getId()], $sortOptions);
        self::assertCount(2, $response);

        $listedJob1 = $response[0];
        self::assertEquals($createdJob1->jsonSerialize(), $listedJob1->jsonSerialize());

        $listedJob2 = $response[1];
        self::assertEquals($createdJob2->jsonSerialize(), $listedJob2->jsonSerialize());

        // sort DESC tests
        $sortOptions = (new JobsSortOptions())
            ->setSortBy('id')
            ->setSortOrder(JobsSortOptions::SORT_ORDER_DESC);

        /** @var Job[] $response */
        $response = $client->getJobsWithIds([$createdJob1->getId(), $createdJob2->getId()], $sortOptions);
        self::assertCount(2, $response);

        $listedJob1 = $response[0];
        self::assertEquals($createdJob2->jsonSerialize(), $listedJob1->jsonSerialize());

        $listedJob2 = $response[1];
        self::assertEquals($createdJob1->jsonSerialize(), $listedJob2->jsonSerialize());
    }

    public function testListJobsByComponent(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $job2 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId3,
            'componentId' => self::COMPONENT_ID_2,
            'mode' => 'run',
        ]);
        $client->createJob($job2);

        // list one component
        $response = $client->listJobs(
            (new JobListOptions())
                ->setComponents([self::COMPONENT_ID_1])
                ->setStatuses([JobFactory::STATUS_CREATED]),
            true
        );
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
        self::assertNull($listedJob->jsonSerialize()['branchId']);

        // list more components
        $response = $client->listJobs(
            (new JobListOptions())
                ->setComponents([self::COMPONENT_ID_1, self::COMPONENT_ID_2])
                ->setStatuses([JobFactory::STATUS_CREATED]),
            true
        );
        self::assertCount(2, $response);
    }

    public function testListJobsSort(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);

        $job2 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob2 = $client->createJob($job2);

        $job3 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob3 = $client->createJob($job3);

        $jobIds = array_map(
            fn (JobInterface $job) => $job->getId(),
            [$createdJob, $createdJob2, $createdJob3]
        );

        $response = $client->listJobs(
            (new JobListOptions())
                ->setIds($jobIds)
                ->setSortBy('id')
                ->setSortOrder(JobListOptions::SORT_ORDER_DESC),
            true
        );
        self::assertCount(3, $response);

        $resIds = array_map(
            fn (JobInterface $job) => $job->getId(),
            $response
        );
        rsort($jobIds);

        self::assertSame($jobIds, $resIds);
    }

    public function testListJobsEscaping(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => '{}=žýřčšěš (*^&^$%£  $"£)?! \'',
            'tag' => 'non-existent',
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);

        $response = $client->listJobs(
            (new JobListOptions())
                ->setComponents(['{}=žýřčšěš (*^&^$%£  $"£)?! \''])
                ->setStatuses([JobFactory::STATUS_CREATED]),
            true
        );
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
    }

    public function testListJobsBranchId(): void
    {
        $branchId = 'branch-' . rand(0, 9999);
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'branchId' => $branchId,
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);

        $job2 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob2 = $client->createJob($job2);

        // all jobs
        $response = $client->listJobs(
            (new JobListOptions())
                ->setStatuses([JobFactory::STATUS_CREATED]),
            true
        );
        self::assertCount(2, $response);

        // job with branch
        $response = $client->listJobs(
            (new JobListOptions())
                ->setStatuses([JobFactory::STATUS_CREATED])
                ->setBranchIds([$branchId]),
            true
        );
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
        self::assertEquals($branchId, $listedJob->jsonSerialize()['branchId']);

        // job without branch
        $response = $client->listJobs(
            (new JobListOptions())
                ->setStatuses([JobFactory::STATUS_CREATED])
                ->setBranchIds(['null']),
            true
        );

        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];

        self::assertEquals($createdJob2->jsonSerialize(), $listedJob->jsonSerialize());
        self::assertNull($listedJob->jsonSerialize()['branchId']);
    }

    public function testListJobsConfigRowIds(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'configRowIds' => ['123'],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);

        $job2 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId2,
            'configRowIds' => ['123', '456'],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob2 = $client->createJob($job2);

        $job3 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job3);

        // jobs with configRowId 123
        $response = $client->listJobs(
            (new JobListOptions())
                ->setStatuses([JobFactory::STATUS_CREATED])
                ->setConfigRowIds(['123']),
            true
        );
        self::assertCount(2, $response);
        /** @var Job $listedJob1 */
        $listedJob1 = $response[1];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob1->jsonSerialize());
        self::assertContains('123', $listedJob1->jsonSerialize()['configRowIds']);
        /** @var Job $listedJob2 */
        $listedJob2 = $response[0];
        self::assertEquals($createdJob2->jsonSerialize(), $listedJob2->jsonSerialize());
        self::assertContains('123', $listedJob2->jsonSerialize()['configRowIds']);

        // jobs with configRowId 123 and config $configId2
        $response = $client->listJobs(
            (new JobListOptions())
                ->setStatuses([JobFactory::STATUS_CREATED])
                ->setConfigs([self::$configId2])
                ->setConfigRowIds(['123']),
            true
        );
        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob2->jsonSerialize(), $listedJob->jsonSerialize());
        self::assertNull($listedJob->jsonSerialize()['branchId']);

        // no jobs
        $response = $client->listJobs(
            (new JobListOptions())
                ->setStatuses([JobFactory::STATUS_CREATED])
                ->setConfigs([self::$configId1])
                ->setConfigRowIds(['456']),
            true
        );
        self::assertEmpty($response);
    }

    public function testGetJobsWithNoIds(): void
    {
        $client = $this->getClient();
        $response = $client->getJobsWithIds([]);
        self::assertCount(0, $response);
    }

    public function testGetJobsWithNoStatuses(): void
    {
        $client = $this->getClient();
        $response = $client->getJobsWithStatus([]);
        self::assertCount(0, $response);
    }

    public function testPostJobResultAndMetrics(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $client = $this->getClient();
        $job = $client->getJob($createdJob->getId());
        self::assertEquals(JobFactory::STATUS_CREATED, $job->getStatus());
        self::assertEquals([], $job->getResult());
        $client->postJobResult(
            $createdJob->getId(),
            JobFactory::STATUS_PROCESSING,
            (new JobResult())->setMessage('bar'),
            (new JobMetrics())->setInputTablesBytesSum(654)->setBackendSize('medium')
                ->setBackendContainerSize('small')
        );
        $job = $client->getJob($createdJob->getId());
        self::assertEquals(JobFactory::STATUS_PROCESSING, $job->getStatus());
        self::assertEquals('bar', $job->getResult()['message']);
        self::assertEquals(654, $job->getMetrics()->getInputTablesBytesSum());
        self::assertEquals('medium', $job->getMetrics()->getBackendSize());
        self::assertEquals('small', $job->getMetrics()->getBackendContainerSize());
    }

    public function testGetJobsWithProjectId(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $response = $client->listJobs(
            (new JobListOptions())->setProjects([$job->getProjectId()])->setIds([$job->getId()]),
            false
        );

        self::assertCount(1, $response);
        /** @var Job $listedJob */
        $listedJob = $response[0];
        self::assertEquals($createdJob->jsonSerialize(), $listedJob->jsonSerialize());
    }

    public function testGetJobsWithProjectIdNonExisting(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);

        $client->createJob($job);
        $client = $this->getClient();
        $response = $client->listJobs(
            (new JobListOptions())
                ->setProjects([$job->getProjectId()])
                ->setComponents(['keboola.non-existent']),
            false
        );

        self::assertCount(0, $response);
    }

    public function testUpdateJobStatusRejected(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $this->expectException(StateTargetEqualsCurrentException::class);
        $this->expectExceptionMessage('Invalid status transition of job');
        $client->updateJob($createdJob);
    }

    public function testUpdateJobDesiredStatus(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);

        $terminatingJob = $client->patchJob(
            $createdJob->getId(),
            (new JobPatchData())->setDesiredStatus(JobFactory::DESIRED_STATUS_TERMINATING)
        );

        self::assertEquals(JobFactory::STATUS_CREATED, $terminatingJob->getStatus());
        self::assertEquals(JobFactory::DESIRED_STATUS_TERMINATING, $terminatingJob->getDesiredStatus());
    }

    public function testPatchJobStatus(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job);

        $client->patchJob(
            $job->getId(),
            (new JobPatchData())->setStatus(JobFactory::STATUS_PROCESSING)
        );
        $processingJob = $client->getJob($job->getId());
        self::assertEquals(JobFactory::STATUS_PROCESSING, $processingJob->getStatus());
    }

    public function testPatchJobDesiredStatus(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job);

        $client->patchJob(
            $job->getId(),
            (new JobPatchData())->setDesiredStatus(JobFactory::DESIRED_STATUS_TERMINATING)
        );
        $terminatingJob = $client->getJob($job->getId());
        self::assertEquals(JobFactory::DESIRED_STATUS_TERMINATING, $terminatingJob->getDesiredStatus());
        // status must not be affected
        self::assertEquals(JobFactory::STATUS_CREATED, $terminatingJob->getStatus());
    }

    public function testPatchJobMultiple(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job);

        $client->patchJob(
            $job->getId(),
            (new JobPatchData())
                ->setStatus(JobFactory::STATUS_TERMINATING)
                ->setDesiredStatus(JobFactory::DESIRED_STATUS_TERMINATING)
        );
        $terminatingJob = $client->getJob($job->getId());
        self::assertEquals(JobFactory::DESIRED_STATUS_TERMINATING, $terminatingJob->getDesiredStatus());
        self::assertEquals(JobFactory::STATUS_TERMINATING, $terminatingJob->getStatus());

        $client->patchJob(
            $job->getId(),
            (new JobPatchData())
                ->setStatus(JobFactory::STATUS_TERMINATED)
                ->setResult((new JobResult())->setMessage('Terminated'))
        );
        $terminatedJob = $client->getJob($job->getId());
        self::assertEquals(JobFactory::DESIRED_STATUS_TERMINATING, $terminatedJob->getDesiredStatus());
        self::assertEquals(JobFactory::STATUS_TERMINATED, $terminatedJob->getStatus());
        self::assertEquals('Terminated', $terminatedJob->getResult()['message']);
    }

    public function testPatchJobStatusRejected(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob = $client->createJob($job);
        $this->expectException(StateTargetEqualsCurrentException::class);
        $this->expectExceptionMessage('Invalid status transition of job');
        $client->patchJob(
            $job->getId(),
            (new JobPatchData())
                ->setStatus($createdJob->getStatus())
                ->setDesiredStatus($createdJob->getDesiredStatus())
        );
    }

    public function testGetJobsDurationSum(): void
    {
        $duration = 2;
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configData' => [],
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);

        $durationSum = $client->getJobsDurationSum($job->getProjectId());

        $job = $client->createJob($job);

        $client->patchJob($job->getId(), (new JobPatchData())->setStatus(JobFactory::STATUS_PROCESSING));

        sleep($duration);

        $job = $client->postJobResult($job->getId(), JobFactory::STATUS_SUCCESS, new JobResult());

        self::assertSame($durationSum + $job->getDurationSeconds(), $client->getJobsDurationSum($job->getProjectId()));
    }
}

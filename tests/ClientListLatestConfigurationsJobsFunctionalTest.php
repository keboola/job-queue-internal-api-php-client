<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\ListLatestConfigurationsJobsOptions;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;

class ClientListLatestConfigurationsJobsFunctionalTest extends BaseClientFunctionalTest
{
    private static string $configId1;
    private static string $configId2;
    private static string $branchId1;
    private const COMPONENT_ID_1 = 'keboola.runner-config-test';
    private const COMPONENT_ID_2 = 'keboola.runner-workspace-test';
    private static Client $client;
    private static Client $masterClient;

    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        $className = substr((string) strrchr(__CLASS__, '\\'), 1);

        self::$client = new Client(
            [
                'token' => (string) getenv('TEST_STORAGE_API_TOKEN'),
                'url' => (string) getenv('TEST_STORAGE_API_URL'),
            ],
        );

        self::$masterClient = new Client(
            [
                'token' => (string) getenv('TEST_STORAGE_API_TOKEN_MASTER'),
                'url' => (string) getenv('TEST_STORAGE_API_URL'),
            ],
        );

        $componentsApi = new Components(self::$client);
        $configuration = new Configuration();
        $configuration->setConfiguration([]);
        $configuration->setComponentId(self::COMPONENT_ID_1);
        $configuration->setName($className);

        self::$configId1 = $componentsApi->addConfiguration($configuration)['id'];
        self::$configId2 = $componentsApi->addConfiguration($configuration)['id'];
        $configuration->setConfigurationId(self::$configId1);
        $configuration->setComponentId(self::COMPONENT_ID_2);
        $componentsApi->addConfiguration($configuration);

        $branchesApi = new DevBranches(self::$masterClient);
        foreach ($branchesApi->listBranches() as $devBranch) {
            if ($devBranch['name'] === $className) {
                $branchesApi->deleteBranch($devBranch['id']);
            }
        }

        $devBranch = $branchesApi->createBranch($className);
        self::$branchId1 = (string) $devBranch['id'];
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
    }

    public function testListJobs(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'branchId' => self::$branchId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job);

        $job2 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId2,
            'branchId' => self::$branchId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $expectedJob2 = $client->createJob($job2);

        $job3 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'branchId' => self::$branchId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $expectedJob3 = $client->createJob($job3);

        $response = $client->listLatestConfigurationsJobs(
            new ListLatestConfigurationsJobsOptions($job->getProjectId(), self::$branchId1),
        );

        self::assertCount(2, $response);
        self::assertEquals($expectedJob3->jsonSerialize(), $response[0]->jsonSerialize());
        self::assertEquals($expectedJob2->jsonSerialize(), $response[1]->jsonSerialize());
    }

    public function testOtherProjectFilter(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job1 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job1);

        $response = $client->listLatestConfigurationsJobs(
            (new ListLatestConfigurationsJobsOptions('other-project')),
        );

        self::assertCount(0, $response);
    }

    public function testPagination(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job1 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job1);
        $job2 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId2,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $expectedJob = $client->createJob($job2);
        $job3 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_2,
            'mode' => 'run',
        ]);
        $client->createJob($job3);

        $response = $client->listLatestConfigurationsJobs(
            (new ListLatestConfigurationsJobsOptions($job1->getProjectId()))
                ->setLimit(1)
                ->setOffset(1),
        );

        self::assertCount(1, $response);
        self::assertEquals($expectedJob->jsonSerialize(), $response[0]->jsonSerialize());
    }

    public function testListJobsWithBranch(): void
    {
        $newJobFactory = $this->getNewJobFactory();
        $client = $this->getClient();

        $job1 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'branchId' => self::$branchId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob1 = $client->createJob($job1);
        $job2 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job2);
        $job3 = $newJobFactory->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId2,
            'branchId' => self::$branchId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob3 = $client->createJob($job3);

        $response = $client->listLatestConfigurationsJobs(
            (new ListLatestConfigurationsJobsOptions($job1->getProjectId(), self::$branchId1)),
        );

        self::assertCount(2, $response);
        self::assertEquals($createdJob3->jsonSerialize(), $response[0]->jsonSerialize());
        self::assertEquals($createdJob1->jsonSerialize(), $response[1]->jsonSerialize());
    }
}

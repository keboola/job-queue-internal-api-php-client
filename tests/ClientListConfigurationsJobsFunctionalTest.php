<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\ListConfigurationsJobsOptions;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;

class ClientListConfigurationsJobsFunctionalTest extends BaseClientFunctionalTest
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
        self::$client = new Client(
            [
                'token' => (string) getenv('TEST_STORAGE_API_TOKEN'),
                'url' => (string) getenv('TEST_STORAGE_API_URL'),
            ]
        );

        self::$masterClient = new Client(
            [
                'token' => (string) getenv('TEST_STORAGE_API_TOKEN_MASTER'),
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
        $configuration->setConfigurationId(self::$configId1);
        $configuration->setComponentId(self::COMPONENT_ID_2);
        $componentsApi->addConfiguration($configuration);

        $devBranch = (new DevBranches(self::$masterClient))->createBranch('ClientListConfigurationsJobsFunctionalTest');
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
        if (self::$branchId1) {
            $branchesApi = new DevBranches(self::$masterClient);
            $branchesApi->deleteBranch((int) self::$branchId1);
        }
    }

    public function testConfigJobsAreListed(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'branchId' => self::$branchId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $expectedJob = $client->createJob($job);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId2,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            new ListConfigurationsJobsOptions([self::$configId1])
        );

        self::assertCount(1, $response);
        self::assertEquals($expectedJob->jsonSerialize(), $response[0]->jsonSerialize());
    }

    public function testLatestConfigJobIsListed(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_2,
            'mode' => 'run',
        ]);
        $expectedJob = $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            new ListConfigurationsJobsOptions([self::$configId1])
        );

        self::assertCount(1, $response);
        self::assertEquals($expectedJob->jsonSerialize(), $response[0]->jsonSerialize());
    }

    public function testMultipleConfigJobsAreListed(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $expectedJob1 = $client->createJob($job1);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_2,
            'mode' => 'run',
        ]);
        $expectedJob2 = $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions([self::$configId1]))
                ->setJobsPerConfig(2)
        );

        self::assertCount(2, $response);
        self::assertEquals($expectedJob2->jsonSerialize(), $response[0]->jsonSerialize());
        self::assertEquals($expectedJob1->jsonSerialize(), $response[1]->jsonSerialize());
    }

    public function testExistingProjectFilter(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $expectedJob1 = $client->createJob($job1);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions([self::$configId1]))
                ->setProjectId($job1->getProjectId())
        );

        self::assertCount(1, $response);
        self::assertEquals($expectedJob1->jsonSerialize(), $response[0]->jsonSerialize());
    }

    public function testOtherProjectFilter(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job1);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions([self::$configId1]))
                ->setProjectId('other-project')
        );

        self::assertCount(0, $response);
    }

    public function testSorting(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $expectedJob1 = $client->createJob($job1);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId2,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $expectedJob2 = $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions([self::$configId1, self::$configId2]))
                ->setSort('componentId')
        );

        self::assertCount(2, $response);
        self::assertEquals($expectedJob1->jsonSerialize(), $response[0]->jsonSerialize());
        self::assertEquals($expectedJob2->jsonSerialize(), $response[1]->jsonSerialize());
    }

    public function testPagination(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $client->createJob($job1);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_2,
            'mode' => 'run',
        ]);
        $expectedJob = $client->createJob($job2);
        $job3 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_2,
            'mode' => 'run',
        ]);
        $client->createJob($job3);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions([self::$configId1]))
                ->setJobsPerConfig(3)
                ->setLimit(1)
                ->setOffset(1)
        );

        self::assertCount(1, $response);
        self::assertEquals($expectedJob->jsonSerialize(), $response[0]->jsonSerialize());
    }

    public function testBranchJobsAreListed(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'branchId' => self::$branchId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob1 = $client->createJob($job1);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob2 = $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions([self::$configId1]))
                ->setJobsPerConfig(1)
                ->setBranchId(self::$branchId1)
        );

        self::assertCount(1, $response);
        self::assertEquals($createdJob1->jsonSerialize(), $response[0]->jsonSerialize());
    }

    public function testJobsWithoutBranchAreListed(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'branchId' => self::$branchId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob1 = $client->createJob($job1);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => self::$configId1,
            'componentId' => self::COMPONENT_ID_1,
            'mode' => 'run',
        ]);
        $createdJob2 = $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions([self::$configId1]))
                ->setJobsPerConfig(1)
                ->setBranchId('null')
        );

        self::assertCount(1, $response);
        self::assertEquals($createdJob2->jsonSerialize(), $response[0]->jsonSerialize());
    }
}

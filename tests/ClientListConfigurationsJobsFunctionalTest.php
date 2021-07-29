<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\ListConfigurationsJobsOptions;

class ClientListConfigurationsJobsFunctionalTest extends BaseClientFunctionalTest
{
    public function testConfigJobsAreListed(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'keboola.ex-google-drive',
            'mode' => 'run',
        ]);
        $expectedJob = $client->createJob($job);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '56789',
            'componentId' => 'keboola.ex-google-analytics',
            'mode' => 'run',
        ]);
        $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            new ListConfigurationsJobsOptions(['12345'])
        );

        self::assertCount(1, $response);
        self::assertEquals($expectedJob->jsonSerialize(), $response[0]->jsonSerialize());
    }

    public function testLatestConfigJobIsListed(): void
    {
        $client = $this->getClient();
        $job = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'keboola.ex-google-drive',
            'mode' => 'run',
        ]);
        $client->createJob($job);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'keboola.ex-google-analytics',
            'mode' => 'run',
        ]);
        $expectedJob = $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            new ListConfigurationsJobsOptions(['12345'])
        );

        self::assertCount(1, $response);
        self::assertEquals($expectedJob->jsonSerialize(), $response[0]->jsonSerialize());
    }

    public function testMultipleConfigJobsAreListed(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'keboola.ex-google-drive',
            'mode' => 'run',
        ]);
        $expectedJob1 = $client->createJob($job1);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'keboola.ex-google-analytics',
            'mode' => 'run',
        ]);
        $expectedJob2 = $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions(['12345']))
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
            'configId' => '12345',
            'componentId' => 'keboola.ex-google-drive',
            'mode' => 'run',
        ]);
        $expectedJob1 = $client->createJob($job1);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions(['12345']))
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
            'configId' => '12345',
            'componentId' => 'keboola.ex-google-drive',
            'mode' => 'run',
        ]);
        $client->createJob($job1);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions(['12345']))
                ->setProjectId('other-project')
        );

        self::assertCount(0, $response);
    }

    public function testSorting(): void
    {
        $client = $this->getClient();
        $job1 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'a-keboola.ex-google-drive',
            'mode' => 'run',
        ]);
        $expectedJob1 = $client->createJob($job1);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '456',
            'componentId' => 'c-keboola.ex-google-analytics',
            'mode' => 'run',
        ]);
        $expectedJob2 = $client->createJob($job2);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions(['12345', '456']))
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
            'configId' => '12345',
            'componentId' => 'a-keboola.ex-google-drive',
            'mode' => 'run',
        ]);
        $client->createJob($job1);
        $job2 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'c-keboola.ex-google-analytics',
            'mode' => 'run',
        ]);
        $expectedJob = $client->createJob($job2);
        $job3 = $client->getJobFactory()->createNewJob([
            '#tokenString' => getenv('TEST_STORAGE_API_TOKEN'),
            'configId' => '12345',
            'componentId' => 'c-keboola.ex-google-analytics',
            'mode' => 'run',
        ]);
        $client->createJob($job3);

        $response = $client->listConfigurationsJobs(
            (new ListConfigurationsJobsOptions(['12345']))
                ->setJobsPerConfig(3)
                ->setLimit(1)
                ->setOffset(1)
        );

        self::assertCount(1, $response);
        self::assertEquals($expectedJob->jsonSerialize(), $response[0]->jsonSerialize());
    }
}

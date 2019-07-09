<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Exception;
use Keboola\JobQueueInternalClient\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;

class JobFactoryTest extends TestCase
{
    public function setUp(): void
    {
        parent::setUp();
        if (empty(getenv('TEST_STORAGE_API_URL')) || empty(getenv('TEST_STORAGE_API_TOKEN'))
            || empty(getenv('TEST_KMS_KEY_ALIAS')) || empty(getenv('TEST_KMS_REGION'))
            || empty(getenv('TEST_AWS_ACCESS_KEY_ID')) || empty(getenv('TEST_AWS_SECRET_ACCESS_KEY'))
        ) {
            throw new Exception('The environment variable "TEST_STORAGE_API_URL" ' .
                'or "TEST_STORAGE_API_TOKEN" or "TEST_KMS_KEY_ALIAS" or "TEST_KMS_REGION" or ' .
                '"TEST_AWS_ACCESS_KEY_ID" or "TEST_AWS_SECRET_ACCESS_KEY" is empty.');
        }
        putenv('AWS_ACCESS_KEY_ID=' . getenv('TEST_AWS_ACCESS_KEY_ID'));
        putenv('AWS_SECRET_ACCESS_KEY=' . getenv('TEST_AWS_SECRET_ACCESS_KEY'));
    }

    private function getJobFactory(): JobFactory
    {
        $storageClientFactory = new JobFactory\StorageClientFactory((string) getenv('TEST_STORAGE_API_URL'));
        $objectEncryptorFactory = new ObjectEncryptorFactory(
            (string) getenv('TEST_KMS_KEY_ALIAS'),
            (string) getenv('TEST_KMS_REGION'),
            '',
            ''
        );
        return new JobFactory($storageClientFactory, $objectEncryptorFactory);
    }

    public function testCreateNewJob(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '123',
                'component' => 'keboola.test',
                'mode' => 'run',
            ],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertEquals('123', $job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getToken());
        self::assertEquals([], $job->getConfigData());
        self::assertNull($job->getRowId());
        self::assertNull($job->getTag());
    }

    public function testCreateNewJobFull(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '123',
                'component' => 'keboola.test',
                'mode' => 'run',
                'row' => '234',
                'configData' => [
                    'parameters' => [
                        'foo' => 'bar',
                    ],
                ],
                'tag' => 'latest',
            ],
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertEquals('123', $job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getToken());
        self::assertEquals('234', $job->getRowId());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
    }

    public function testStaticGetters(): void
    {
        self::assertCount(5, JobFactory::getFinishedStatuses());
        self::assertCount(9, JobFactory::getAllStatuses());
    }

    public function testModifyJob(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '123',
                'component' => 'keboola.test',
                'mode' => 'run',
                'tag' => 'latest',
                'configData' => [
                    'parameters' => [
                        'foo' => 'bar',
                    ],
                ],
            ],
        ];
        $job = $factory->createNewJob($data);
        $newJob = $factory->modifyJob($job, ['params' => ['config' => '345'], 'status' => 'waiting']);
        self::assertNotEmpty($job->getId());
        self::assertEquals('345', $newJob->getConfigId());
        self::assertEquals('123', $job->getConfigId());
        self::assertEquals('waiting', $newJob->getStatus());
        self::assertEquals('created', $job->getStatus());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getToken());
        self::assertNull($job->getRowId());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
    }

    public function testCreateInvalidJob(): void
    {
        $jobData = [
            'params' => [
                'config' => '123',
                'component' => 'keboola.test',
                'mode' => 'run',
            ],
        ];
        self::expectException(ClientException::class);
        self::expectExceptionMessage('The child node "token" at path "job" must be configured.');
        $this->getJobFactory()->createNewJob($jobData);
    }
}

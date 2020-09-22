<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\ObjectEncryptor\Legacy\Encryptor;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\ObjectEncryptor\Wrapper\ComponentWrapper;
use Keboola\ObjectEncryptor\Wrapper\ConfigurationWrapper;
use Keboola\ObjectEncryptor\Wrapper\ProjectWrapper;
use Keboola\StorageApi\Client;

class JobFactoryTest extends BaseTest
{
    public function setUp(): void
    {
        parent::setUp();
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
            '123456789012345678901234567890ab'
        );
        return new JobFactory($storageClientFactory, $objectEncryptorFactory);
    }

    public function testDummy(): void
    {
        self::assertEquals(true, true);
    }

    public function testCreateNewJob(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => '123',
            'component' => 'keboola.test',
            'mode' => 'run',
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertEquals('123', $job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getToken());
        self::assertEquals([], $job->getConfigData());
        self::assertEquals(getenv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
        self::assertEquals([], $job->getConfigDataDecrypted());
        self::assertNull($job->getRowId());
        self::assertNull($job->getTag());
        self::assertEquals($job->getId(), $job->getRunId());
    }

    public function testCreateNewJobNormalize(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => 123,
            'component' => 123,
            'mode' => 'run',
            'tag' => 123,
            'row' => 123,
            'parentRunId' => 1234.567,
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getToken());
        self::assertSame([], $job->getConfigData());
        self::assertSame(getenv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
        self::assertSame([], $job->getConfigDataDecrypted());
        self::assertSame('123', $job->getConfigId());
        self::assertSame('123', $job->getRowId());
        self::assertSame('123', $job->getTag());
        self::assertSame('1234.567.' . $job->getId() , $job->getRunId());
        self::assertSame('1234.567', $job->getParentRunId());
        self::assertSame('123', $job->jsonSerialize()['params']['config']);
        self::assertSame('123', $job->jsonSerialize()['params']['row']);
        self::assertSame('123', $job->jsonSerialize()['params']['tag']);
        self::assertSame('1234.567.' . $job->getId(), $job->jsonSerialize()['runId']);
    }

    public function testGetTokenLegacyDecrypted(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => '123',
            'component' => 'keboola.test',
            'mode' => 'run',
        ];
        $job = $factory->createNewJob($data);
        $reflection = new \ReflectionProperty(Job::class, 'data');
        $reflection->setAccessible(true);
        $data = $reflection->getValue($job);
        $encryptor = new Encryptor('123456789012345678901234567890ab');
        $data['token']['token'] = $encryptor->encrypt('someToken');
        $reflection->setValue($job, $data);
        self::assertNotEquals('someToken', $job->getToken());
        self::assertEquals('someToken', $job->getTokenDecrypted());
    }

    public function testCreateNewJobFull(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'parentRunId' => '2345',
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
        ];
        $job = $factory->createNewJob($data);
        self::assertNotEmpty($job->getId());
        self::assertEquals('123', $job->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure::', $job->getToken());
        self::assertEquals('234', $job->getRowId());
        self::assertEquals(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertEquals('latest', $job->getTag());
        self::assertEquals('2345.' . $job->getId(), $job->getRunId());
    }

    public function testLoadLegacyOrchestratorJob(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'id' => '664651692',
            'runId' => '664651695',
            'lockName' => 'orchestrator-12345',
            'project' => [
                'id' => '219',
            ],
            'token' => [
                'id' => '12345',
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'component' => 'orchestrator',
            'command' => 'run',
            'params' => [
                'config' => 456789,
                'mode' => 'run',
                'row' => null,
                'tag' => null,
            ],
            'status' => 'waiting',
            'createdTime' => '2020-01-09T12:46:08.164Z',
        ];
        $job = $factory->loadFromExistingJobData($data);
        self::assertNotEmpty($job->getId());
        self::assertSame('456789', $job->getConfigId());
        self::assertSame(null, $job->getRowId());
        self::assertSame([], $job->getConfigData());
        self::assertSame(null, $job->getTag());
        self::assertSame('orchestrator', $job->getComponentId());
    }

    public function testLoadInvalidJob(): void
    {
        $jobData = [
            'id' => '664651692',
            'status' => 'waiting',
            'params' => [
                'config' => '123',
                'mode' => 'run',
            ],
            'project' => [
                'id' => '219',
            ],
            'token' => [
                'id' => '12345',
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
        ];
        self::expectException(ClientException::class);
        self::expectExceptionMessage('The child node "component" at path "job.params" must be configured.');
        $this->getJobFactory()->loadFromExistingJobData($jobData);
    }

    public function testLoadLegacyTransformationsJob(): void
    {
        $jobData = [
            'id' => 138361,
            'runId' => '138362',
            'lockName' => 'transformation-21-137869-run',
            'project' => [
                'id' => 21,
                'name' => 'Odin - Queue',
            ],
            'token' => [
                'id' => '127',
                'description' => 'john.doe@keboola.com',
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'component' => 'transformation',
            'command' => 'run',
            'params' => [
                'call' => 'run',
                'mode' => 'full',
                'phases' => [],
                'transformations' => [],
                'config' => '137869',
                'configBucketId' => '137869',
            ],
            'result' => [],
            'status' => 'waiting',
            'process' => [
                'host' => 'ip-10-0-41-225.us-east-2.compute.internal',
                'pid' => 96,
            ],
            'createdTime' => '2020-01-17T10:21:14+01:00',
            'startTime' => null,
            'endTime' => null,
            'durationSeconds' => null,
            'waitSeconds' => null,
            'nestingLevel' => 0,
            'isFinished' => false,
            '_index' => null,
            '_type' => null,
            'url' => 'https://queue.east-us-2.azure.keboola.com/jobs/138361',
        ];
        $job = $this->getJobFactory()->loadFromExistingJobData($jobData);
        $jobData['params']['component'] = 'transformation';
        $jobData['params']['row'] = null;
        $jobData['params']['tag'] = null;
        self::assertEquals($jobData, $job->jsonSerialize());
    }

    public function testStaticGetters(): void
    {
        self::assertCount(5, JobFactory::getFinishedStatuses());
        self::assertCount(9, JobFactory::getAllStatuses());
        self::assertCount(3, JobFactory::getLegacyComponents());
        self::assertCount(3, JobFactory::getKillableStatuses());
    }

    public function testModifyJob(): void
    {
        $factory = $this->getJobFactory();
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => '123',
            'component' => 'keboola.test',
            'mode' => 'run',
            'tag' => 'latest',
            'configData' => [
                'parameters' => [
                    'foo' => 'bar',
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
            'config' => '123',
            'component' => 'keboola.test',
            'mode' => 'run',
        ];
        self::expectException(ClientException::class);
        self::expectExceptionMessage('The child node "token" at path "job" must be configured.');
        $this->getJobFactory()->createNewJob($jobData);
    }

    public function testCreateInvalidToken(): void
    {
        $data = [
            'token' => 'invalid',
            'config' => '123',
            'component' => 'keboola.test',
            'mode' => 'run',
        ];
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Cannot create job: "Invalid access token".');
        $this->getJobFactory()->createNewJob($data);
    }

    public function testEncryption(): void
    {
        $objectEncryptorFactory = new ObjectEncryptorFactory(
            (string) getenv('TEST_KMS_KEY_ALIAS'),
            (string) getenv('TEST_KMS_REGION'),
            '',
            ''
        );
        $client = new Client(
            [
                'url' => getenv('TEST_STORAGE_API_URL'),
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ]
        );
        $tokenInfo = $client->verifyToken();
        $objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $objectEncryptorFactory->setConfigurationId('123');
        $objectEncryptorFactory->setComponentId('keboola.test');
        $objectEncryptorFactory->setStackId(parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST));

        $factory = $this->getJobFactory();
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => '123',
            'configData' => [
                '#foo1' => $objectEncryptorFactory->getEncryptor()->encrypt('bar1', ProjectWrapper::class),
                '#foo2' => $objectEncryptorFactory->getEncryptor()->encrypt('bar2', ComponentWrapper::class),
                '#foo3' => $objectEncryptorFactory->getEncryptor()->encrypt('bar3', ConfigurationWrapper::class),
            ],
            'component' => 'keboola.test',
            'mode' => 'run',
        ];
        $job = $factory->createNewJob($data);
        self::assertStringStartsWith('KBC::ProjectSecure', $job->getConfigData()['#foo1']);
        self::assertStringStartsWith('KBC::ComponentSecure', $job->getConfigData()['#foo2']);
        self::assertStringStartsWith('KBC::ConfigSecure', $job->getConfigData()['#foo3']);
        self::assertEquals(
            [
                '#foo1' => 'bar1',
                '#foo2' => 'bar2',
                '#foo3' => 'bar3',
            ],
            $job->getConfigDataDecrypted()
        );
    }

    public function testEncryptionMultipleJobs(): void
    {
        /* this test does basically the same as testEncryption() method, but with two different jobs and
        ObjectEncryptor settings. Because ObjectEncryptorFactory is not immutable (legacy reasons), it has to
        be cloned inside the Job class before it is modified. This method actually tests that it is cloned (i.e. two
        jobs do not interfere with each other). */
        $objectEncryptorFactory = new ObjectEncryptorFactory(
            (string) getenv('TEST_KMS_KEY_ALIAS'),
            (string) getenv('TEST_KMS_REGION'),
            '',
            ''
        );
        $storageClientFactory = new JobFactory\StorageClientFactory((string) getenv('TEST_STORAGE_API_URL'));
        $client = new Client(
            [
                'url' => getenv('TEST_STORAGE_API_URL'),
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ]
        );
        $tokenInfo = $client->verifyToken();
        $objectEncryptorFactory->setStackId(parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST));

        $objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $objectEncryptorFactory->setConfigurationId('123');
        $objectEncryptorFactory->setComponentId('keboola.test1');
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => '123',
            'configData' => [
                '#foo11' => $objectEncryptorFactory->getEncryptor()->encrypt('bar11', ProjectWrapper::class),
                '#foo12' => $objectEncryptorFactory->getEncryptor()->encrypt('bar12', ComponentWrapper::class),
                '#foo13' => $objectEncryptorFactory->getEncryptor()->encrypt('bar13', ConfigurationWrapper::class),
            ],
            'component' => 'keboola.test1',
            'mode' => 'run',
        ];
        $jobFactory1 = new JobFactory($storageClientFactory, $objectEncryptorFactory);
        $job1 = $jobFactory1->createNewJob($data);

        $objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $objectEncryptorFactory->setConfigurationId('456');
        $objectEncryptorFactory->setComponentId('keboola.test2');
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => '456',
            'configData' => [
                '#foo21' => $objectEncryptorFactory->getEncryptor()->encrypt('bar21', ProjectWrapper::class),
                '#foo22' => $objectEncryptorFactory->getEncryptor()->encrypt('bar22', ComponentWrapper::class),
                '#foo23' => $objectEncryptorFactory->getEncryptor()->encrypt('bar23', ConfigurationWrapper::class),
            ],
            'component' => 'keboola.test2',
            'mode' => 'run',
        ];
        $jobFactory2 = new JobFactory($storageClientFactory, $objectEncryptorFactory);
        $job2 = $jobFactory2->createNewJob($data);

        self::assertEquals('123', $job1->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job1->getConfigData()['#foo11']);
        self::assertStringStartsWith('KBC::ComponentSecure', $job1->getConfigData()['#foo12']);
        self::assertStringStartsWith('KBC::ConfigSecure', $job1->getConfigData()['#foo13']);
        self::assertEquals(
            [
                '#foo11' => 'bar11',
                '#foo12' => 'bar12',
                '#foo13' => 'bar13',
            ],
            $job1->getConfigDataDecrypted()
        );

        self::assertEquals('456', $job2->getConfigId());
        self::assertStringStartsWith('KBC::ProjectSecure', $job2->getConfigData()['#foo21']);
        self::assertStringStartsWith('KBC::ComponentSecure', $job2->getConfigData()['#foo22']);
        self::assertStringStartsWith('KBC::ConfigSecure', $job2->getConfigData()['#foo23']);
        self::assertEquals(
            [
                '#foo21' => 'bar21',
                '#foo22' => 'bar22',
                '#foo23' => 'bar23',
            ],
            $job2->getConfigDataDecrypted()
        );
    }

    public function testEncryptionFactoryIsolation(): void
    {
        /* this test does basically the same as testEncryption() method, but with two different jobs and
        ObjectEncryptor settings. Because ObjectEncryptorFactory is not immutable (legacy reasons), it has to
        be cloned inside the Job class before it is modified. This method actually tests that it is cloned (i.e. two
        jobs do not interfere with each other). */
        $objectEncryptorFactory = new ObjectEncryptorFactory(
            (string) getenv('TEST_KMS_KEY_ALIAS'),
            (string) getenv('TEST_KMS_REGION'),
            '',
            ''
        );
        $client = new Client(
            [
                'url' => getenv('TEST_STORAGE_API_URL'),
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ]
        );
        $tokenInfo = $client->verifyToken();
        $objectEncryptorFactory->setStackId(parse_url((string) getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST));

        $objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $objectEncryptorFactory->setConfigurationId('456');
        $objectEncryptorFactory->setComponentId('keboola.different-test');
        $encrypted = $objectEncryptorFactory->getEncryptor()->encrypt('bar', ProjectWrapper::class);
        $storageClientFactory = new JobFactory\StorageClientFactory((string) getenv('TEST_STORAGE_API_URL'));
        $jobFactory = new JobFactory($storageClientFactory, $objectEncryptorFactory);
        $data = [
            'token' => getenv('TEST_STORAGE_API_TOKEN'),
            'config' => '123',
            'component' => 'keboola.test1',
            'mode' => 'run',
        ];
        $jobFactory->createNewJob($data);
        self::assertStringStartsWith('KBC::ProjectSecure', $encrypted);
        self::assertEquals('bar', $objectEncryptorFactory->getEncryptor()->decrypt($encrypted));
    }
}

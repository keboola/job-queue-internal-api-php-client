<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
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
            ''
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
        self::assertEquals(getenv('TEST_STORAGE_API_TOKEN'), $job->getTokenDecrypted());
        self::assertEquals([], $job->getConfigDataDecrypted());
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

    public function testCreateInvalidToken(): void
    {
        $data = [
            'token' => [
                'token' => 'invalid',
            ],
            'params' => [
                'config' => '123',
                'component' => 'keboola.test',
                'mode' => 'run',
            ],
        ];
        self::expectException(ClientException::class);
        self::expectExceptionMessage('Cannot create job: Invalid access token');
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
        $objectEncryptorFactory->setStackId(parse_url(getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST));

        $factory = $this->getJobFactory();
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '123',
                'configData' => [
                    '#foo1' => $objectEncryptorFactory->getEncryptor()->encrypt('bar1', ProjectWrapper::class),
                    '#foo2' => $objectEncryptorFactory->getEncryptor()->encrypt('bar2', ComponentWrapper::class),
                    '#foo3' => $objectEncryptorFactory->getEncryptor()->encrypt('bar3', ConfigurationWrapper::class),
                ],
                'component' => 'keboola.test',
                'mode' => 'run',
            ],
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
        $objectEncryptorFactory->setStackId(parse_url(getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST));

        $objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $objectEncryptorFactory->setConfigurationId('123');
        $objectEncryptorFactory->setComponentId('keboola.test1');
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '123',
                'configData' => [
                    '#foo11' => $objectEncryptorFactory->getEncryptor()->encrypt('bar11', ProjectWrapper::class),
                    '#foo12' => $objectEncryptorFactory->getEncryptor()->encrypt('bar12', ComponentWrapper::class),
                    '#foo13' => $objectEncryptorFactory->getEncryptor()->encrypt('bar13', ConfigurationWrapper::class),
                ],
                'component' => 'keboola.test1',
                'mode' => 'run',
            ],
        ];
        $jobFactory1 = new JobFactory($storageClientFactory, $objectEncryptorFactory);
        $job1 = $jobFactory1->createNewJob($data);

        $objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $objectEncryptorFactory->setConfigurationId('456');
        $objectEncryptorFactory->setComponentId('keboola.test2');
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '456',
                'configData' => [
                    '#foo21' => $objectEncryptorFactory->getEncryptor()->encrypt('bar21', ProjectWrapper::class),
                    '#foo22' => $objectEncryptorFactory->getEncryptor()->encrypt('bar22', ComponentWrapper::class),
                    '#foo23' => $objectEncryptorFactory->getEncryptor()->encrypt('bar23', ConfigurationWrapper::class),
                ],
                'component' => 'keboola.test2',
                'mode' => 'run',
            ],
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
        $objectEncryptorFactory->setStackId(parse_url(getenv('TEST_STORAGE_API_URL'), PHP_URL_HOST));

        $objectEncryptorFactory->setProjectId($tokenInfo['owner']['id']);
        $objectEncryptorFactory->setConfigurationId('456');
        $objectEncryptorFactory->setComponentId('keboola.different-test');
        $encrypted = $objectEncryptorFactory->getEncryptor()->encrypt('bar', ProjectWrapper::class);
        $storageClientFactory = new JobFactory\StorageClientFactory((string) getenv('TEST_STORAGE_API_URL'));
        $jobFactory = new JobFactory($storageClientFactory, $objectEncryptorFactory);
        $data = [
            'token' => [
                'token' => getenv('TEST_STORAGE_API_TOKEN'),
            ],
            'params' => [
                'config' => '123',
                'component' => 'keboola.test1',
                'mode' => 'run',
            ],
        ];
        $jobFactory->createNewJob($data);
        self::assertStringStartsWith('KBC::ProjectSecure', $encrypted);
        self::assertEquals('bar', $objectEncryptorFactory->getEncryptor()->decrypt($encrypted));
    }
}

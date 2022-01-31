<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\Exception\PermissionsException;
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\PermissionChecker;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use PHPUnit\Framework\TestCase;

class PermissionCheckerTest extends TestCase
{
    private function getJob(): Job
    {
        $jobData = [
            'id' => '123456456',
            'runId' => '123456456',
            'configId' => '454124290',
            'componentId' => 'keboola.dummy',
            'mode' => 'run',
            'configData' => [
                'parameters' => ['foo' => 'bar'],
            ],
            'status' => 'created',
            'desiredStatus' => 'processing',
            'projectId' => '123',
            'tokenId' => '456',
            '#tokenString' => 'KBC::ProjectSecure::token',
        ];
        $objectEncryptorFactoryMock = self::getMockBuilder(ObjectEncryptorFactory::class)
            ->disableOriginalConstructor()
            ->getMock();
        /** @var ObjectEncryptorFactory $objectEncryptorFactoryMock */
        return new Job($objectEncryptorFactoryMock, $jobData);
    }

    /**
     * @param JobInterface $job
     * @param array $tokenInfo
     * @dataProvider allowedJobsProvider
     */
    public function testJobRunAllowed(JobInterface $job, array $tokenInfo): void
    {
        PermissionChecker::verifyJobRunPermissions($job, $tokenInfo);
        self::assertTrue(true);
    }

    public function allowedJobsProvider(): array
    {
        return [
            'componentUnset' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                        'role' => 'admin',
                    ],
                    'owner' => [
                        'id' => '123',
                        'features' => ['abcd', 'queuev2'],
                    ],
                    'id' => '123',
                ],
            ],
            'componentValid' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                        'role' => 'admin',
                    ],
                    'owner' => [
                        'id' => '123',
                        'features' => ['abcd', 'queuev2'],
                    ],
                    'id' => '123',
                    'componentAccess' => [
                        'keboola.dummy', 'keboola.dummy-2',
                    ],
                ],
            ],
            'roleUnset' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                    ],
                    'owner' => [
                        'id' => '123',
                        'features' => ['abcd', 'queuev2'],
                    ],
                    'id' => '123',
                    'componentAccess' => [
                        'keboola.dummy', 'keboola.dummy-2',
                    ],
                ],
            ],
            'roleAdmin' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                        'role' => 'admin',
                    ],
                    'owner' => [
                        'id' => '123',
                        'features' => ['abcd', 'queuev2'],
                    ],
                    'id' => '123',
                ],
            ],
        ];
    }

    /**
     * @param JobInterface $job
     * @param array $tokenInfo
     * @param string $expectedError
     * @dataProvider forbiddenJobsProvider
     */
    public function testJobRunForbidden(JobInterface $job, array $tokenInfo, string $expectedError): void
    {
        self::expectException(PermissionsException::class);
        self::expectExceptionMessage($expectedError);
        PermissionChecker::verifyJobRunPermissions($job, $tokenInfo);
    }

    public function forbiddenJobsProvider(): array
    {
        return [
            'componentNotMatching' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                        'role' => 'admin',
                    ],
                    'owner' => [
                        'id' => '123',
                        'name' => 'test',
                        'features' => ['abcd', 'queuev2'],
                    ],
                    'id' => '123',
                    'componentAccess' => [
                        'keboola.not-dummy', 'keboola.not-dummy-2',
                    ],
                ],
                'message' => 'You do not have permission to run jobs of "keboola.dummy" component.',
            ],
            'roleReadOnly' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                        'role' => 'readOnly',
                    ],
                    'owner' => [
                        'id' => '123',
                        'name' => 'test',
                        'features' => ['abcd', 'queuev2'],
                    ],
                    'id' => '123',
                    'componentAccess' => [
                        'keboola.dummy', 'keboola.dummy-2',
                    ],
                ],
                'message' => 'You have read only access to the project, you cannot run any jobs.',
            ],
            'roleReadOnly2' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                        'role' => 'readonly',
                    ],
                    'owner' => [
                        'id' => '123',
                        'name' => 'test',
                        'features' => ['abcd', 'queuev2'],
                    ],
                    'id' => '123',
                    'componentAccess' => [
                        'keboola.dummy', 'keboola.dummy-2',
                    ],
                ],
                'message' => 'You have read only access to the project, you cannot run any jobs.',
            ],
            'featureMissing' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                        'role' => 'readonly',
                    ],
                    'owner' => [
                        'id' => '123',
                        'name' => 'test',
                        'features' => ['abcd'],
                    ],
                    'id' => '123',
                    'componentAccess' => [
                        'keboola.dummy', 'keboola.dummy-2',
                    ],
                ],
                'message' => 'Feature "queuev2" is not enabled in the project "test" (id: 123).',
            ],
        ];
    }
}

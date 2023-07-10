<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests;

use Keboola\JobQueueInternalClient\JobFactory\Job;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptor\JobObjectEncryptor;
use Keboola\JobQueueInternalClient\PermissionChecker;
use Keboola\PermissionChecker\BranchType;
use Keboola\PermissionChecker\Exception\PermissionDeniedException;
use Keboola\PermissionChecker\StorageApiTokenInterface;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
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
            'branchType' => BranchType::DEFAULT->value,
        ];
        $objectEncryptorMock = $this->createMock(JobObjectEncryptor::class);
        $storageFactoryMock = $this->createMock(StorageClientPlainFactory::class);
        return new Job($objectEncryptorMock, $storageFactoryMock, $jobData);
    }

    /**
     * @dataProvider allowedJobsProvider
     */
    public function testJobRunAllowed(JobInterface $job, array $tokenInfo): void
    {
        $token = new class($tokenInfo) implements StorageApiTokenInterface {
            private array $tokenInfo;

            public function __construct(array $tokenInfo)
            {
                $this->tokenInfo = $tokenInfo;
            }

            public function getFeatures(): array
            {
                return $this->tokenInfo['owner']['features'];
            }

            public function getRole(): ?string
            {
                return $this->tokenInfo['admin']['role'] ?? null;
            }

            public function getAllowedComponents(): ?array
            {
                return $this->tokenInfo['componentAccess'] ?? null;
            }
        };


        PermissionChecker::verifyJobRunPermissions($job, $token);
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
        $token = new class($tokenInfo) implements StorageApiTokenInterface {
            private array $tokenInfo;

            public function __construct(array $tokenInfo)
            {
                $this->tokenInfo = $tokenInfo;
            }

            public function getFeatures(): array
            {
                return $this->tokenInfo['owner']['features'];
            }

            public function getRole(): ?string
            {
                return $this->tokenInfo['admin']['role'] ?? null;
            }

            public function getAllowedComponents(): ?array
            {
                return $this->tokenInfo['componentAccess'] ?? null;
            }
        };

        $this->expectException(PermissionDeniedException::class);
        $this->expectExceptionMessage($expectedError);
        PermissionChecker::verifyJobRunPermissions($job, $token);
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
                'message' => 'Token is not allowed to run component "keboola.dummy"',
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
                'message' => 'Role "readOnly" is not allowed to run jobs',
            ],
            'featureMissing' => [
                'job' => $this->getJob(),
                'tokenInfo' => [
                    'admin' => [
                        'name' => 'John doe',
                        'role' => 'readOnly',
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
                'message' => 'Project does not have feature "queuev2" enabled',
            ],
        ];
    }
}

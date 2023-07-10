<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\PermissionChecker\Check\JobQueue\CanRunJob;
use Keboola\PermissionChecker\StorageApiTokenInterface;

class PermissionChecker
{
    public static function verifyJobRunPermissions(JobInterface $job, StorageApiTokenInterface $token): void
    {
        $checker = new \Keboola\PermissionChecker\PermissionChecker();

        assert($job->getBranchType() !== null);
        $checker->checkPermissions($token, new CanRunJob($job->getBranchType(), $job->getComponentId()));
    }
}

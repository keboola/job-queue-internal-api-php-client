<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\PermissionsException;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;

class PermissionChecker
{
    public static function verifyJobRunPermissions(JobInterface $job, array $tokenInfo): void
    {
        if (empty($tokenInfo['owner']['features']) || !in_array('queuev2', $tokenInfo['owner']['features'])) {
            throw new PermissionsException(sprintf(
                'Feature "queuev2" is not enabled in the project "%s" (id: %s).',
                $tokenInfo['owner']['name'],
                $tokenInfo['owner']['id']
            ));
        }
        if (!empty($tokenInfo['componentAccess']) && !in_array($job->getComponentId(), $tokenInfo['componentAccess'])) {
            throw new PermissionsException(
                sprintf('You do not have permission to run jobs of "%s" component.', $job->getComponentId())
            );
        }
        if (!empty($tokenInfo['admin']['role']) &&
            (strcasecmp($tokenInfo['admin']['role'], 'readOnly') === 0)
        ) {
            throw new PermissionsException('You have read only access to the project, you cannot run any jobs.');
        }
    }
}

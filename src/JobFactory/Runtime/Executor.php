<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\Runtime;

enum Executor: string
{
    case DIND = 'dind';
    case K8S_CONTAINERS = 'k8sContainers';

    public static function getDefault(): self
    {
        return self::DIND;
    }
}

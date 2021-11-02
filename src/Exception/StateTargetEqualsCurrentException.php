<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Exception;

class StateTargetEqualsCurrentException extends ClientException
{
    public const STRING_CODE = 'statusTargetEqualsCurrent';
}

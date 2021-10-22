<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Exception;

class StateTransitionForbiddenException extends ClientException
{
    public const STRING_CODE = 'statusTransitionForbidden';
}

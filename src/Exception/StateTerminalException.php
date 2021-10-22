<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Exception;

class StateTerminalException extends ClientException
{
    public const STRING_CODE = 'statusTerminal';
}

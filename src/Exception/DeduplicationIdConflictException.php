<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Exception;

class DeduplicationIdConflictException extends ClientException
{
    public const STRING_CODE = 'dbDeduplicationIdConflict';
}

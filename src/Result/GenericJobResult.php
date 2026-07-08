<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;

final class GenericJobResult implements JsonSerializable
{
    /**
     * @param array<mixed> $data
     */
    public function __construct(
        private readonly array $data,
    ) {
    }

    /**
     * @return array<mixed>
     */
    public function jsonSerialize(): array
    {
        return $this->data;
    }
}

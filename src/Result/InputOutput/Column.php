<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result\InputOutput;

use JsonSerializable;

class Column implements JsonSerializable
{
    private string $name;

    public function __construct(string $name)
    {
        $this->name = $name;
    }

    public function getName(): string
    {
        return $this->name;
    }
    public function jsonSerialize(): array
    {
        return [
            'name' => $this->name,
        ];
    }
}

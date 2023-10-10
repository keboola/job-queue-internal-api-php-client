<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result\Variable;

use JsonSerializable;

class Variable implements JsonSerializable
{
    public function __construct(
        private readonly string $name,
        private readonly string $value,
    ) {
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getValue(): string
    {
        return $this->value;
    }

    public function jsonSerialize(): array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
        ];
    }
}

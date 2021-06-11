<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

class Backend
{
    /** @var null|string */
    private $type;

    public function __construct(?string $type)
    {
        $this->type = $type;
    }

    public static function fromDataArray(array $data): self
    {
        return new self(
            $data['type'] ?? null
        );
    }

    public function getType(): ?string
    {
        return $this->type;
    }
}

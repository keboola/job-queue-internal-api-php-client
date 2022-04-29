<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

class Backend
{
    private ?string $type;
    private ?string $containerType;

    public function __construct(?string $type, ?string $containerType)
    {
        $this->type = $type;
        $this->containerType = $containerType;
    }

    public static function fromDataArray(array $data): self
    {
        return new self(
            $data['type'] ?? null,
            $data['containerType'] ?? null
        );
    }

    public function toDataArray(): array
    {
        return ['type' => $this->type, 'containerType' => $this->containerType];
    }

    public function getType(): ?string
    {
        return $this->type;
    }

    public function getContainerType(): ?string
    {
        return $this->containerType;
    }

    public function isEmpty(): bool
    {
        return $this->type === null && $this->containerType === null;
    }
}

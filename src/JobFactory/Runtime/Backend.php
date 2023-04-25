<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\Runtime;

class Backend
{
    private const OPTION_TYPE = 'type';
    private const OPTION_CONTAINER_TYPE = 'containerType';
    private const OPTION_CONTEXT = 'context';

    private ?string $type;
    private ?string $containerType;
    private ?string $context;

    public function __construct(
        ?string $type,
        ?string $containerType,
        ?string $context
    ) {
        $this->type = $type;
        $this->containerType = $containerType;
        $this->context = $context;
    }

    public static function fromDataArray(array $data): self
    {
        return new self(
            $data[self::OPTION_TYPE] ?? null,
            $data[self::OPTION_CONTAINER_TYPE] ?? null,
            $data[self::OPTION_CONTEXT] ?? null
        );
    }

    public function toDataArray(): array
    {
        return [
            self::OPTION_TYPE => $this->type,
            self::OPTION_CONTAINER_TYPE => $this->containerType,
            self::OPTION_CONTEXT => $this->context,
        ];
    }

    public function getType(): ?string
    {
        return $this->type;
    }

    public function getContainerType(): ?string
    {
        return $this->containerType;
    }

    public function getContext(): ?string
    {
        return $this->context;
    }

    public function isEmpty(): bool
    {
        return $this->type === null && $this->containerType === null && $this->context === null;
    }
}

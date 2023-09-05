<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

class Behavior
{
    public const ON_ERROR_STOP = 'stop';
    public const ON_ERROR_WARNING = 'warning';
    public const ON_ERROR_FAIL = 'fail';

    private ?string $onError;

    public function __construct(?string $onError = null)
    {
        $this->onError = $onError;
    }

    public static function fromDataArray(array $data): self
    {
        return new self(
            $data['onError'] ?? null,
        );
    }

    public function toDataArray(): array
    {
        return [
            'onError' => $this->onError,
        ];
    }

    public function getOnError(): ?string
    {
        return $this->onError;
    }

    public static function getBehaviorTypes(): array
    {
        return [self::ON_ERROR_STOP, self::ON_ERROR_WARNING, self::ON_ERROR_FAIL];
    }
}

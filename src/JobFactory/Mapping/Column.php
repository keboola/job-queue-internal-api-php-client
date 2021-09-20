<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\Mapping;

use InvalidArgumentException;
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

    public static function fromDataArray(array $data): self
    {
        self::assertNotEmpty('name', $data);

        return new self(
            $data['name'],
        );
    }

    private static function assertNotEmpty(string $key, array $data): void
    {
        if (empty($data[$key])) {
            throw new InvalidArgumentException(sprintf('Empty value or missing data for "%s".', $key));
        }
    }
}

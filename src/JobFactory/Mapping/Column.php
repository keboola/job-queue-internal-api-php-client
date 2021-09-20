<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\Mapping;

use InvalidArgumentException;

class Column
{
    private string $id;
    private string $name;
    private string $displayName;

    public function __construct(string $id, string $name, string $displayName)
    {
        $this->id = $id;
        $this->name = $name;
        $this->displayName = $displayName;
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getDisplayName(): string
    {
        return $this->displayName;
    }

    public function jsonSerialize(): array
    {
        return [
            'id' => $this->id,
            'name' => $this->name,
            'displayName' => $this->displayName,
        ];
    }

    public static function fromDataArray(array $data): self
    {
        self::assertNotEmpty('id', $data);
        self::assertNotEmpty('name', $data);
        self::assertNotEmpty('displayName', $data);

        return new self(
            $data['id'],
            $data['name'],
            $data['displayName'],
        );
    }

    private static function assertNotEmpty(string $key, array $data): void
    {
        if (empty($data[$key])) {
            throw new InvalidArgumentException(sprintf('Empty value or missing data for "%s".', $key));
        }
    }
}

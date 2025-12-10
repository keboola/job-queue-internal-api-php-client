<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;

class VariableValues
{
    /** @var string|null */
    private $variableValuesId;
    /** @var array{values?: list<array{name: string, value: string}>} */
    private $variableValuesData;

    /**
     * @param array{values?: list<array{name: string, value: string}>} $variableValuesData
     */
    public function __construct(?string $variableValuesId, array $variableValuesData)
    {
        $this->variableValuesId = $variableValuesId;
        $this->variableValuesData = $variableValuesData;
        if (!empty($this->variableValuesId) && !$this->isValuesEmpty()) {
            throw new ClientException(
                'Provide either "variableValuesId" or "variableValuesData", but not both.',
            );
        }
    }

    /**
     * @param array{
     *     variableValuesId?: string|null,
     *     variableValuesData?: array{values?: list<array{name: string, value: string}>}
     * } $data
     */
    public static function fromDataArray(array $data): self
    {
        return new self(
            $data['variableValuesId'] ?? null,
            $data['variableValuesData'] ?? [],
        );
    }

    /**
     * @return array{
     *     variableValuesId: string|null,
     *     variableValuesData: array{values?: list<array{name: string, value: string}>}
     * }
     */
    public function asDataArray(): array
    {
        return [
            'variableValuesId' => $this->variableValuesId,
            'variableValuesData' => $this->variableValuesData,
        ];
    }

    public function getValuesId(): ?string
    {
        return $this->variableValuesId;
    }

    public function getValuesData(): array
    {
        return $this->variableValuesData;
    }

    public function isEmpty(): bool
    {
        return empty($this->getValuesId()) && $this->isValuesEmpty();
    }

    private function isValuesEmpty(): bool
    {
        return empty($this->getValuesData()['values']);
    }
}

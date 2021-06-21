<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;

class VariableValues
{
    /** @var string|null */
    private $variableValuesId;
    /** @var array */
    private $variableValuesData;

    public function __construct(?string $variableValuesId, array $variableValuesData)
    {
        if (!empty($variableValuesId) && !empty($variableValuesData)) {
            throw new ClientException(
                'Provide either "variableValuesId" or "variableValuesData", but not both.'
            );
        }

        $this->variableValuesId = $variableValuesId;
        $this->variableValuesData = $variableValuesData;
    }

    public static function fromDataArray(array $data): self
    {
        return new self(
            $data['variableValuesId'] ?? null,
            $data['variableValuesData'] ?? []
        );
    }

    public function asDataArray(): array
    {
        $data = [];
        if ($this->variableValuesId) {
            $data['variableValuesId'] = $this->variableValuesId;
        }
        if (!empty($this->variableValuesData['values'])) {
            $data['variableValuesData'] = $this->variableValuesData;
        }
        return $data;
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
        return empty($this->getValuesId()) &&
            (empty($this->getValuesData()) || empty($this->getValuesData()['values']));
    }
}

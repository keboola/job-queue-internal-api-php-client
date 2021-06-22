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
        $this->variableValuesId = $variableValuesId;
        $this->variableValuesData = $variableValuesData;
        if (!empty($this->variableValuesId) && !$this->isValuesEmpty()) {
            throw new ClientException(
                'Provide either "variableValuesId" or "variableValuesData", but not both.'
            );
        }
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
        if (!$this->isValuesEmpty()) {
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
        return empty($this->getValuesId()) && $this->isValuesEmpty();
    }

    private function isValuesEmpty(): bool
    {
        return empty($this->getValuesData()) || empty($this->getValuesData()['values']);
    }
}

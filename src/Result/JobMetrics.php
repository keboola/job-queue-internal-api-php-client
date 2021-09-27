<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;

class JobMetrics implements JsonSerializable
{
    private ?int $inputTablesBytesSum = null;

    public function jsonSerialize(): array
    {
        return [
            'storage' => [
                'inputTablesBytesSum' => $this->inputTablesBytesSum,
            ],
        ];
    }

    public function getInputTablesBytesSum(): ?int
    {
        return $this->inputTablesBytesSum;
    }

    public function setInputTablesBytesSum(int $bytes): JobMetrics
    {
        $this->inputTablesBytesSum = $bytes;
        return $this;
    }

    public static function fromDataArray(array $data): self
    {
        $metricsData = $data['metrics'] ?? [];
        $metrics = new self();
        if (isset($metricsData['storage']['inputTablesBytesSum'])) {
            $metrics->setInputTablesBytesSum($metricsData['storage']['inputTablesBytesSum']);
        }
        return $metrics;
    }
}

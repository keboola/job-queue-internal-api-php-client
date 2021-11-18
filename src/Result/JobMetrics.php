<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;

class JobMetrics implements JsonSerializable
{
    private ?int $inputTablesBytesSum = null;

    private ?string $backendSize = null;

    public function jsonSerialize(): array
    {
        return [
            'storage' => [
                'inputTablesBytesSum' => $this->inputTablesBytesSum,
            ],
            'backend' => [
                'size' => $this->backendSize,
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

    public function getBackendSize(): ?string
    {
        return $this->backendSize;
    }

    public function setBackendSize(string $size): JobMetrics
    {
        $this->backendSize = $size;
        return $this;
    }

    public static function fromDataArray(array $data): self
    {
        $metricsData = $data['metrics'] ?? [];
        $metrics = new self();
        if (isset($metricsData['storage']['inputTablesBytesSum'])) {
            $metrics->setInputTablesBytesSum($metricsData['storage']['inputTablesBytesSum']);
        }
        if (isset($metricsData['backend']['size'])) {
            $metrics->setBackendSize($metricsData['backend']['size']);
        }
        return $metrics;
    }
}
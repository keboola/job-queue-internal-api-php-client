<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;

class JobMetrics implements JsonSerializable
{
    private ?int $inputTablesBytesSum = null;
    private ?int $outputTablesBytesSum = null;

    private ?string $backendSize = null;
    private ?string $backendContainerSize = null;

    public function jsonSerialize(): array
    {
        return [
            'storage' => [
                'inputTablesBytesSum' => $this->inputTablesBytesSum,
                'outputTablesBytesSum' => $this->outputTablesBytesSum,
            ],
            'backend' => [
                'size' => $this->backendSize,
                'containerSize' => $this->backendContainerSize,
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

    public function getBackendContainerSize(): ?string
    {
        return $this->backendContainerSize;
    }

    public function setBackendContainerSize(string $size): JobMetrics
    {
        $this->backendContainerSize = $size;
        return $this;
    }

    public static function fromDataArray(array $data): self
    {
        $metricsData = $data['metrics'] ?? [];
        $metrics = new self();
        if (isset($metricsData['storage']['inputTablesBytesSum'])) {
            $metrics->setInputTablesBytesSum($metricsData['storage']['inputTablesBytesSum']);
        }
        if (isset($metricsData['storage']['outputTablesBytesSum'])) {
            $metrics->setOutputTablesBytesSum($metricsData['storage']['outputTablesBytesSum']);
        }
        if (isset($metricsData['backend']['size'])) {
            $metrics->setBackendSize($metricsData['backend']['size']);
        }
        if (isset($metricsData['backend']['containerSize'])) {
            $metrics->setBackendContainerSize($metricsData['backend']['containerSize']);
        }
        return $metrics;
    }

    public function setOutputTablesBytesSum(int $bytes): JobMetrics
    {
        $this->outputTablesBytesSum = $bytes;
        return $this;
    }

    public function getOutputTablesBytesSum(): ?int
    {
        return $this->outputTablesBytesSum;
    }
}

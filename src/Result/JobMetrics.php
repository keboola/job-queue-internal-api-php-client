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
    private ?string $backendContext = null;

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
                'context' => $this->backendContext,
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

    public function getBackendContext(): ?string
    {
        return $this->backendContext;
    }

    public function setBackendContext(string $context): JobMetrics
    {
        $this->backendContext = $context;
        return $this;
    }

    public static function fromDataArray(array $data): self
    {
        $metricsData = isset($data['metrics']) && is_array($data['metrics']) ? $data['metrics'] : [];
        $storageMetrics = isset($metricsData['storage']) && is_array($metricsData['storage'])
            ? $metricsData['storage']
            : [];
        $backendMetrics = isset($metricsData['backend']) && is_array($metricsData['backend'])
            ? $metricsData['backend']
            : [];

        $metrics = new self();

        if (isset($storageMetrics['inputTablesBytesSum']) && is_scalar($storageMetrics['inputTablesBytesSum'])) {
            $metrics->setInputTablesBytesSum((int) $storageMetrics['inputTablesBytesSum']);
        }
        if (isset($storageMetrics['outputTablesBytesSum']) && is_scalar($storageMetrics['outputTablesBytesSum'])) {
            $metrics->setOutputTablesBytesSum((int) $storageMetrics['outputTablesBytesSum']);
        }

        if (isset($backendMetrics['size']) && is_scalar($backendMetrics['size'])) {
            $metrics->setBackendSize((string) $backendMetrics['size']);
        }
        if (isset($backendMetrics['containerSize']) && is_scalar($backendMetrics['containerSize'])) {
            $metrics->setBackendContainerSize((string) $backendMetrics['containerSize']);
        }
        if (isset($backendMetrics['context']) && is_scalar($backendMetrics['context'])) {
            $metrics->setBackendContext((string) $backendMetrics['context']);
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

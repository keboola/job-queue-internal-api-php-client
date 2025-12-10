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
        $metrics = new self();
        if (
            isset($metricsData['storage']) && is_array($metricsData['storage']) &&
            isset($metricsData['storage']['inputTablesBytesSum']) &&
            is_scalar($metricsData['storage']['inputTablesBytesSum'])
        ) {
            $metrics->setInputTablesBytesSum((int) $metricsData['storage']['inputTablesBytesSum']);
        }
        if (
            isset($metricsData['storage']) && is_array($metricsData['storage']) &&
            isset($metricsData['storage']['outputTablesBytesSum']) &&
            is_scalar($metricsData['storage']['outputTablesBytesSum'])
        ) {
            $metrics->setOutputTablesBytesSum((int) $metricsData['storage']['outputTablesBytesSum']);
        }
        if (
            isset($metricsData['backend']) && is_array($metricsData['backend']) &&
            isset($metricsData['backend']['size']) &&
            is_scalar($metricsData['backend']['size'])
        ) {
            $metrics->setBackendSize((string) $metricsData['backend']['size']);
        }
        if (
            isset($metricsData['backend']) && is_array($metricsData['backend']) &&
            isset($metricsData['backend']['containerSize']) &&
            is_scalar($metricsData['backend']['containerSize'])
        ) {
            $metrics->setBackendContainerSize((string) $metricsData['backend']['containerSize']);
        }
        if (
            isset($metricsData['backend']) && is_array($metricsData['backend']) &&
            isset($metricsData['backend']['context']) &&
            is_scalar($metricsData['backend']['context'])
        ) {
            $metrics->setBackendContext((string) $metricsData['backend']['context']);
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

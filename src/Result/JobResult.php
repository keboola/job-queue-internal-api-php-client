<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Result\InputOutput\TableCollection;

class JobResult implements JsonSerializable
{
    private ?string $message = null;
    private ?array $images = null;
    private ?string $configVersion = null;
    private ?string $errorType = null;
    private ?string $exceptionId = null;

    public const ERROR_TYPE_APPLICATION = 'application';
    public const ERROR_TYPE_USER = 'user';

    private ?TableCollection $inputTables = null;
    private ?TableCollection $outputTables = null;

    public function jsonSerialize(): array
    {
        $result = [
            'message' => $this->message,
            'configVersion' => $this->configVersion,
            'images' => $this->images,
        ];
        if ($this->inputTables) {
            $result['input']['tables'] = $this->inputTables->jsonSerialize();
        }
        if ($this->outputTables) {
            $result['output']['tables'] = $this->outputTables->jsonSerialize();
        }
        if ($this->errorType) {
            $result['error']['type'] = $this->errorType;
        }
        if ($this->exceptionId) {
            $result['error']['exceptionId'] = $this->exceptionId;
        }

        return $result;
    }

    private function validateErrorType(string $errorType): void
    {
        if (!in_array($errorType, [self::ERROR_TYPE_APPLICATION, self::ERROR_TYPE_USER])) {
            throw new ClientException(sprintf('Invalid error type: "%s".', $errorType));
        }
    }

    public function setMessage(string $message): JobResult
    {
        $this->message = $message;
        return $this;
    }

    public function getMessage(): ?string
    {
        return $this->message;
    }

    public function setImages(array $images): JobResult
    {
        $this->images = $images;
        return $this;
    }

    public function getImages(): ?array
    {
        return $this->images;
    }

    public function setConfigVersion(string $configVersion): JobResult
    {
        $this->configVersion = $configVersion;
        return $this;
    }

    public function getConfigVersion(): ?string
    {
        return $this->configVersion;
    }

    public function setErrorType(string $errorType): JobResult
    {
        $this->validateErrorType($errorType);
        $this->errorType = $errorType;
        return $this;
    }

    public function getErrorType(): ?string
    {
        return $this->errorType;
    }

    public function setExceptionId(string $exceptionId): JobResult
    {
        $this->exceptionId = $exceptionId;
        return $this;
    }

    public function getExceptionId(): ?string
    {
        return $this->exceptionId;
    }

    public function setInputTables(TableCollection $collection): JobResult
    {
        $this->inputTables = $collection;
        return $this;
    }

    public function getInputTables(): ?TableCollection
    {
        return $this->inputTables;
    }

    public function setOutputTables(TableCollection $collection): JobResult
    {
        $this->outputTables = $collection;
        return $this;
    }

    public function getOutputTables(): ?TableCollection
    {
        return $this->outputTables;
    }
}

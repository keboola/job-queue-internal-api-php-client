<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Result\InputOutput\TableCollection;
use Keboola\JobQueueInternalClient\Result\Variable\Variable;
use Keboola\JobQueueInternalClient\Result\Variable\VariableCollection;

class JobResult implements JsonSerializable
{
    public const ERROR_TYPE_APPLICATION = 'application';
    public const ERROR_TYPE_USER = 'user';

    private ?string $message = null;
    private array $images = [];
    private ?string $configVersion = null;
    private ?string $errorType = null;
    private ?string $exceptionId = null;

    private ?TableCollection $inputTables = null;
    private ?TableCollection $outputTables = null;

    private ?JobArtifacts $artifacts = null;
    private ?VariableCollection $variables = null;

    public function jsonSerialize(): array
    {
        $result = [
            'message' => $this->message,
            'configVersion' => $this->configVersion,
            'images' => $this->images,
            'input' => [
                'tables' => [],
            ],
            'output' => [
                'tables' => [],
            ],
        ];
        if ($this->inputTables) {
            $result['input']['tables'] = $this->inputTables->jsonSerialize();
        }
        if ($this->outputTables) {
            $result['output']['tables'] = $this->outputTables->jsonSerialize();
        }
        if ($this->artifacts) {
            $result['artifacts'] = $this->artifacts->jsonSerialize();
        }
        if ($this->variables) {
            $result['variables'] = $this->variables->jsonSerialize();
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

    public function getImages(): array
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

    public function setArtifacts(JobArtifacts $artifacts): JobResult
    {
        $this->artifacts = $artifacts;
        return $this;
    }

    public function getArtifacts(): ?JobArtifacts
    {
        return $this->artifacts;
    }

    public function setVariables(VariableCollection $variables): JobResult
    {
        $this->variables = $variables;
        return $this;
    }

    public function getVariables(): ?VariableCollection
    {
        return $this->variables;
    }
}

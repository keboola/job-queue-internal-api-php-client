<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use JsonSerializable;
use Keboola\JobQueueInternalClient\Exception\ClientException;

class JobResult implements JsonSerializable
{
    private string $message;
    private array $images;
    private string $configVersion;
    private string $errorType;
    private string $exceptionId;

    public const ERROR_TYPE_APPLICATION = 'application';
    public const ERROR_TYPE_USER = 'user';

    public function jsonSerialize(): array
    {
        $result = [
            'message' => $this->message,
            'configVersion' => $this->configVersion,
            'images' => $this->images,
        ];
        if ($this->errorType) {
            $result['error']['type'] = $this->errorType;
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

    public function getMessage(): string
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

    public function getConfigVersion(): string
    {
        return $this->configVersion;
    }

    public function setErrorType(string $errorType): JobResult
    {
        $this->validateErrorType($errorType);
        $this->errorType = $errorType;
        return $this;
    }

    public function getErrorType(): string
    {
        return $this->errorType;
    }

    public function setExceptionId(string $exceptionId): JobResult
    {
        $this->exceptionId = $exceptionId;
        return $this;
    }

    public function getExceptionId(): string
    {
        return $this->exceptionId;
    }

}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\StorageApi\Client as StorageApiClient;
use Keboola\StorageApiBranch\ClientWrapper as StorageClientWrapper;
use Psr\Log\LoggerInterface;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\Validation;

class StorageClientFactory
{
    private string $storageApiUrl;
    private LoggerInterface $logger;

    public function __construct(string $storageApiUrl, LoggerInterface $logger)
    {
        $validator = Validation::createValidator();
        $errors = $validator->validate($storageApiUrl, [new Url(['message' => 'Storage API URL is not valid.'])]);
        if ($errors->count() !== 0) {
            throw new ClientException(
                'Value "' . $errors->get(0)->getInvalidValue() . '" is invalid: ' . $errors->get(0)->getMessage()
            );
        }
        $this->storageApiUrl = $storageApiUrl;
        $this->logger = $logger;
    }

    public function getClientWrapper(string $token, ?string $branch): StorageClientWrapper
    {
        return new StorageClientWrapper(
            new StorageApiClient(
                [
                    'url' => $this->storageApiUrl,
                    'token' => $token,
                ]
            ),
            null,
            $this->logger,
            is_null($branch) ? StorageClientWrapper::BRANCH_MAIN : $branch
        );
    }

    public function getStorageApiUrl(): string
    {
        return $this->storageApiUrl;
    }
}

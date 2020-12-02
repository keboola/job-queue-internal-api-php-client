<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\StorageApi\Client as StorageApiClient;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\Validation;

class StorageClientFactory
{
    /** @var string */
    private $storageApiUrl;

    public function __construct(string $storageApiUrl)
    {
        $validator = Validation::createValidator();
        $errors = $validator->validate($storageApiUrl, [new Url(['message' => 'Storage API URL is not valid.'])]);
        if ($errors->count() !== 0) {
            throw new ClientException(
                'Value "' . $errors->get(0)->getInvalidValue() . '" is invalid: ' . $errors->get(0)->getMessage()
            );
        }
        $this->storageApiUrl = $storageApiUrl;
    }

    public function getClient(string $token): StorageApiClient
    {
        return new StorageApiClient(['url' => $this->storageApiUrl, 'token' => $token]);
    }

    public function getStorageApiUrl(): string
    {
        return $this->storageApiUrl;
    }
}

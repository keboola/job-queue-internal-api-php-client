<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\ObjectEncryptor\ObjectEncryptorFactory;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Psr\Log\LoggerInterface;

class ClientFactory
{
    private string $internalApiUrl;
    private string $internalApiToken;
    private string $region;
    private string $kmsKeyId;
    private string $akvUrl;
    private LoggerInterface $logger;
    private StorageClientPlainFactory $storageClientPlainFactory;

    public function __construct(
        string $internalApiUrl,
        string $internalApiToken,
        string $region,
        string $kmsKeyId,
        string $akvUrl,
        StorageClientPlainFactory $storageClientPlainFactory,
        LoggerInterface $logger
    ) {
        $this->internalApiUrl = $internalApiUrl;
        $this->internalApiToken = $internalApiToken;
        $this->region = $region;
        $this->kmsKeyId = $kmsKeyId;
        $this->logger = $logger;
        $this->akvUrl = $akvUrl;
        $this->storageClientPlainFactory = $storageClientPlainFactory;
    }

    public function getClient(): Client
    {
        $objectEncryptorFactory = new ObjectEncryptorFactory(
            $this->kmsKeyId,
            $this->region,
            '',
            '',
            $this->akvUrl
        );
        $objectEncryptorFactory->setStackId((string) parse_url(
            (string) $this->storageClientPlainFactory->getClientOptionsReadOnly()->getUrl(),
            PHP_URL_HOST
        ));
        $jobFactory = new JobFactory($this->storageClientPlainFactory, $objectEncryptorFactory);

        return new Client(
            $this->logger,
            $jobFactory,
            $this->internalApiUrl,
            $this->internalApiToken,
            [
                'backoffMaxTries' => 2,
                'userAgent' => 'Public API',
            ]
        );
    }
}

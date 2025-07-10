<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApi\ClientException as StorageApiClientException;
use Keboola\StorageApi\Components as ComponentsApiClient;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Tokens;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Symfony\Component\Uid\Uuid;

class Job extends PlainJob implements JobInterface
{
    private ?ComponentsApiClient $componentsApiClient = null;
    private ?ComponentSpecification $componentSpecification = null;

    private ?string $tokenDecrypted = null;
    private ?string $executionTokenDecrypted = null;
    private ?array $componentConfigurationDecrypted = null;
    private ?array $configDataDecrypted = null;

    private ?array $componentConfiguration = null;
    private ?array $projectFeatures = null;

    public function __construct(
        private readonly JobObjectEncryptor $objectEncryptor,
        private readonly StorageClientPlainFactory $storageClientFactory,
        array $data,
    ) {
        parent::__construct($data);
    }

    public function getTokenDecrypted(): string
    {
        return $this->tokenDecrypted ??= $this->objectEncryptor->decrypt(
            $this->getTokenString(),
            $this->getComponentId(),
            $this->getProjectId(),
            $this->getConfigId(),
            $this->getBranchType(),
        );
    }

    public function getExecutionTokenDecrypted(string $applicationToken): string
    {
        if (in_array(JobFactory::PROTECTED_DEFAULT_BRANCH_FEATURE, $this->getProjectFeatures())
            && ($this->getBranchType() === BranchType::DEFAULT)
        ) {
            return $this->executionTokenDecrypted ??= $this->createPrivilegedToken($applicationToken);
        }

        return $this->getTokenDecrypted();
    }

    public function getComponentConfigurationDecrypted(): ?array
    {
        if ($this->getConfigId() === null) {
            return null;
        }

        return $this->componentConfigurationDecrypted ??= $this->objectEncryptor->decrypt(
            $this->getComponentConfiguration(),
            $this->getComponentId(),
            $this->getProjectId(),
            $this->getConfigId(),
            $this->getBranchType(),
        );
    }

    public function getConfigDataDecrypted(): array
    {
        return $this->configDataDecrypted ??= $this->objectEncryptor->decrypt(
            $this->getConfigData(),
            $this->getComponentId(),
            $this->getProjectId(),
            $this->getConfigId(),
            $this->getBranchType(),
        );
    }

    public function getComponentSpecification(): ComponentSpecification
    {
        if ($this->componentSpecification !== null) {
            return $this->componentSpecification;
        }

        try {
            $data = $this->getComponentsApiClient()->getComponent($this->getComponentId());
        } catch (StorageApiClientException $e) {
            throw new ClientException('Failed to fetch component specification: '.$e->getMessage(), 0, $e);
        }

        return $this->componentSpecification = new ComponentSpecification($data);
    }

    public function getComponentConfiguration(): array
    {
        if ($this->componentConfiguration !== null) {
            return $this->componentConfiguration;
        }
        return $this->componentConfiguration = JobConfigurationResolver::resolveJobConfiguration(
            $this,
            $this->getComponentsApiClient(),
        );
    }

    private function getComponentsApiClient(): ComponentsApiClient
    {
        if ($this->componentsApiClient !== null) {
            return $this->componentsApiClient;
        }

        return $this->componentsApiClient = new ComponentsApiClient(
            $this->getStorageClientWrapper()->getBranchClient(),
        );
    }

    public function getProjectFeatures(): array
    {
        if ($this->projectFeatures !== null) {
            return $this->projectFeatures;
        }

        return $this->projectFeatures = $this->getStorageClientWrapper()
            ->getBranchClient()
            ->verifyToken()['owner']['features'];
    }

    private function createPrivilegedToken(string $applicationToken): string
    {
        $tokens = new Tokens($this->getStorageClientWrapper()->getBasicClient());
        $options = new TokenCreateOptions();
        $options->setDescription(sprintf('Execution Token for job %s', $this->getId()));
        $options->setCanManageBuckets(true);
        $options->setCanReadAllFileUploads(true);
        $options->setExpiresIn(self::EXECUTION_TOKEN_TIMEOUT_SECONDS);
        $token = $tokens->createTokenPrivilegedInProtectedDefaultBranch($options, $applicationToken);

        return $token['token'];
    }

    public static function generateRunnerId(): string
    {
        return (string) Uuid::v4();
    }

    private function getStorageClientWrapper(): ClientWrapper
    {
        return $this->storageClientFactory->createClientWrapper(
            new ClientOptions(null, $this->getTokenDecrypted(), $this->getBranchId()),
        );
    }
}

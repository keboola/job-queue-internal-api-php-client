<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Components;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class JobRuntimeResolver
{
    /** @var StorageClientFactory */
    private $storageClientFactory;
    /** @var LoggerInterface */
    private $logger;
    /** @var JobFactory */
    private $jobFactory;
    /** @var ?Client */
    private $storageClient;
    /** @var ?array */
    private $configuration;
    /** @var ?array */
    private $configData;

    public function __construct(
        LoggerInterface $logger,
        StorageClientFactory $storageClientFactory,
        JobFactory $jobFactory
    ) {
        $this->logger = $logger;
        $this->storageClientFactory = $storageClientFactory;
        $this->jobFactory = $jobFactory;
    }

    public function resolve(JobInterface $job): JobInterface
    {
        $this->configuration = null;
        $this->configData = null;
        $this->storageClient = null;

        try {
            $tag = $this->resolveTag($job);
            $variableValues = $this->resolveVariables($job);
            $backend = $this->resolveBackend($job);
            $patchData = $variableValues->asDataArray();
            $patchData['backend'] = $backend->asDataArray();
            $patchData['tag'] = $tag;
        } catch (InvalidConfigurationException $e) {
            throw new ClientException('Invalid configuration: ' . $e->getMessage(), 0, $e);
        }
        $this->logger->info(sprintf('Resolved component tag to "%s".', $tag));
        return $this->jobFactory->modifyJob($job, $patchData);
    }

    private function resolveTag(JobInterface $job): string
    {
        if ($job->getTag()) {
            return (string) $job->getTag();
        }
        if (!empty($this->getConfigData($job)['runtime']['tag'])) {
            return (string) $this->getConfigData($job)['runtime']['tag'];
        }
        $configuration = $this->getConfiguration($job);
        if (!empty($configuration['runtime']['tag'])) {
            return (string) $configuration['runtime']['tag'];
        }
        $componentsApi = new Components($this->getStorageApiClient($job));
        return $componentsApi->getComponent($job->getComponentId())['data']['definition']['tag'];
    }

    private function resolveVariables(JobInterface $job): VariableValues
    {
        if (!$job->getVariableValues()->isEmpty()) {
            return $job->getVariableValues();
        }
        if (!empty($this->getConfigData($job))) {
            $variableValues = VariableValues::fromDataArray($this->getConfigData($job));
            if (!$variableValues->isEmpty()) {
                return $variableValues;
            }
        }
        $configuration = $this->getConfiguration($job);
        // return these irrespective if they are empty, because if they are we'd create empty VariableValues anyway
        return VariableValues::fromDataArray($configuration);
    }

    private function resolveBackend(JobInterface $job): Backend
    {
        if (!$job->getBackend()->isEmpty()) {
            return $job->getBackend();
        }
        if (!empty($this->getConfigData($job)['runtime']['backend'])) {
            $backend = Backend::fromDataArray($this->getConfigData($job)['runtime']['backend']);
            if (!$backend->isEmpty()) {
                return $backend;
            }
        }
        $configuration = $this->getConfiguration($job);
        if (!empty($configuration['runtime']['backend'])) {
            // return this irrespective if it is empty, because if it is we create empty Backend anyway
            return Backend::fromDataArray($configuration['runtime']['backend']);
        }
        return new Backend(null);
    }

    private function getConfigData(JobInterface $job): array
    {
        if ($this->configData === null) {
            $configurationDefinition = new OverridesConfigurationDefinition();
            $this->configData = $configurationDefinition->processData($job->getConfigData());
        }
        return $this->configData;
    }

    private function getConfiguration(JobInterface $job): array
    {
        if ($this->configuration === null) {
            $componentsApi = new Components($this->getStorageApiClient($job));
            $this->configuration = $componentsApi->getConfiguration(
                $job->getComponentId(),
                $job->getConfigId()
            );
            $configurationDefinition = new OverridesConfigurationDefinition();
            $this->configuration = $configurationDefinition->processData($this->configuration['configuration']);
        }
        return $this->configuration;
    }

    private function getStorageApiClient(JobInterface $job): Client
    {
        if ($this->storageClient === null) {
            $this->storageClient = $this->storageClientFactory->getClient($job->getTokenDecrypted());
        }
        return $this->storageClient;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException as StorageClientException;
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
    /** @var JobInterface */
    private $job;

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
        $this->storageClient = null;
        $this->job = $job;

        try {
            $tag = $this->resolveTag();
            $variableValues = $this->resolveVariables();
            $backend = $this->resolveBackend();
            $patchData = $variableValues->asDataArray();
            $patchData['backend'] = $backend->asDataArray();
            $patchData['tag'] = $tag;
            $this->logger->info(sprintf('Resolved component tag to "%s".', $tag));
            return $this->jobFactory->modifyJob($this->job, $patchData);
        } catch (InvalidConfigurationException $e) {
            throw new ClientException('Invalid configuration: ' . $e->getMessage(), 0, $e);
        } catch (StorageClientException $e) {
            throw new ClientException('Cannot resolve job parameters: ' . $e->getMessage(), 0, $e);
        }
    }

    private function resolveTag(): string
    {
        if ($this->job->getTag()) {
            return (string) $this->job->getTag();
        }
        if (!empty($this->getConfigData()['runtime']['tag'])) {
            return (string) $this->getConfigData()['runtime']['tag'];
        }
        $configuration = $this->getConfiguration($this->job);
        if (!empty($configuration['runtime']['tag'])) {
            return (string) $configuration['runtime']['tag'];
        }
        $componentsApi = new Components($this->getStorageApiClient($this->job));
        return $componentsApi->getComponent($this->job->getComponentId())['data']['definition']['tag'];
    }

    private function resolveVariables(): VariableValues
    {
        if (!$this->job->getVariableValues()->isEmpty()) {
            return $this->job->getVariableValues();
        }
        if (!empty($this->getConfigData())) {
            $variableValues = VariableValues::fromDataArray($this->getConfigData());
            if (!$variableValues->isEmpty()) {
                return $variableValues;
            }
        }
        $configuration = $this->getConfiguration($this->job);
        // return these irrespective if they are empty, because if they are we'd create empty VariableValues anyway
        return VariableValues::fromDataArray($configuration);
    }

    private function resolveBackend(): Backend
    {
        if (!$this->job->getBackend()->isEmpty()) {
            return $this->job->getBackend();
        }
        if (!empty($this->getConfigData()['runtime']['backend'])) {
            $backend = Backend::fromDataArray($this->getConfigData()['runtime']['backend']);
            if (!$backend->isEmpty()) {
                return $backend;
            }
        }
        $configuration = $this->getConfiguration($this->job);
        if (!empty($configuration['runtime']['backend'])) {
            // return this irrespective if it is empty, because if it is we create empty Backend anyway
            return Backend::fromDataArray($configuration['runtime']['backend']);
        }
        return new Backend(null);
    }

    private function getConfigData(): array
    {
        $configurationDefinition = new OverridesConfigurationDefinition();
        return $configurationDefinition->processData($this->job->getConfigData());
    }

    private function getConfiguration(JobInterface $job): array
    {
        if ($this->configuration === null) {
            if ($job->getConfigId()) {
                $componentsApi = new Components($this->getStorageApiClient($job));
                $this->configuration = $componentsApi->getConfiguration(
                    $job->getComponentId(),
                    $job->getConfigId()
                );
                $configurationDefinition = new OverridesConfigurationDefinition();
                $this->configuration = $configurationDefinition->processData($this->configuration['configuration']);
            } else {
                $this->configuration = [];
            }
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

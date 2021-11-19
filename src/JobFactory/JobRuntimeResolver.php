<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Client as InternalApiClient;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApi\Components;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class JobRuntimeResolver
{
    /** @var StorageClientFactory */
    private $storageClientFactory;
    /** @var JobFactory */
    private $jobFactory;
    /** @var ?Components */
    private $componentsApiClient;
    /** @var ?array */
    private $configuration;
    /** @var JobInterface */
    private $job;

    public function __construct(
        StorageClientFactory $storageClientFactory,
        InternalApiClient $internalApiClient
    ) {
        $this->storageClientFactory = $storageClientFactory;
        $this->jobFactory = $internalApiClient->getJobFactory();
    }

    public function resolve(JobInterface $job): JobInterface
    {
        $this->configuration = null;
        $this->componentsApiClient = null;
        $this->job = $job;

        try {
            $tag = $this->resolveTag();
            $variableValues = $this->resolveVariables();
            $backend = $this->resolveBackend();
            $parallelism = $this->resolveParallelism();
            $patchData = $variableValues->asDataArray();
            $patchData['backend'] = $backend->toDataArray();
            $patchData['tag'] = $tag;
            $patchData['parallelism'] = $parallelism;
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
        $configuration = $this->getConfiguration();
        if (!empty($configuration['runtime']['tag'])) {
            return (string) $configuration['runtime']['tag'];
        }
        if (!empty($configuration['runtime']['image_tag'])) {
            return (string) $configuration['runtime']['image_tag'];
        }
        $componentsApi = $this->getComponentsApiClient();
        $componentData = $componentsApi->getComponent($this->job->getComponentId());
        if (!empty($componentData['data']['definition']['tag'])) {
            return $componentData['data']['definition']['tag'];
        } else {
            throw new ClientException(sprintf('The component "%s" is not runnable.', $this->job->getComponentId()));
        }
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
        $configuration = $this->getConfiguration();
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
        $configuration = $this->getConfiguration();
        if (!empty($configuration['runtime']['backend'])) {
            // return this irrespective if it is empty, because if it is we create empty Backend anyway
            return Backend::fromDataArray($configuration['runtime']['backend']);
        }
        return new Backend(null);
    }

    private function resolveParallelism(): ?string
    {
        if (!empty($this->job->getParallelism())) {
            return (string) $this->job->getParallelism();
        }
        if (!empty($this->getConfigData()['runtime']['parallelism'])) {
            return (string) $this->getConfigData()['runtime']['parallelism'];
        }
        $configuration = $this->getConfiguration();
        if (!empty($configuration['runtime']['parallelism'])) {
            return $configuration['runtime']['parallelism'];
        }
        return null;
    }

    private function getConfigData(): array
    {
        $configurationDefinition = new OverridesConfigurationDefinition();
        return $configurationDefinition->processData($this->job->getConfigData());
    }

    private function getConfiguration(): array
    {
        if ($this->configuration === null) {
            if ($this->job->getConfigId()) {
                $componentsApi = $this->getComponentsApiClient();
                $this->configuration = $componentsApi->getConfiguration(
                    $this->job->getComponentId(),
                    $this->job->getConfigId()
                );
                $configurationDefinition = new OverridesConfigurationDefinition();
                $this->configuration = $configurationDefinition->processData($this->configuration['configuration']);
            } else {
                $this->configuration = [];
            }
        }
        return $this->configuration;
    }

    private function getComponentsApiClient(): Components
    {
        if ($this->componentsApiClient === null) {
            $this->componentsApiClient = new Components(
                $this->storageClientFactory->getClient($this->job->getTokenDecrypted())
            );
        }
        return $this->componentsApiClient;
    }
}

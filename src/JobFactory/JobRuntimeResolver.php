<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\ConfigurationDisabledException;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApiBranch\ClientWrapper;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

/**
 * @internal
 */
class JobRuntimeResolver
{
    private StorageClientFactory $storageClientFactory;
    private ?array $configuration;
    private array $jobData;

    public function __construct(StorageClientFactory $storageClientFactory)
    {
        $this->storageClientFactory = $storageClientFactory;
    }

    public function resolveJobData(array $jobData): array
    {
        $this->configuration = null;
        $this->jobData = $jobData;

        try {
            $tag = $this->resolveTag();
            $variableValues = $this->resolveVariables();
            $backend = $this->resolveBackend();
            $parallelism = $this->resolveParallelism();
            foreach ($variableValues->asDataArray() as $key => $value) {
                $jobData[$key] = $value;
            }
            $jobData['backend'] = $backend->toDataArray();
            $jobData['tag'] = $tag;
            $jobData['parallelism'] = $parallelism;

            if ($this->resolveIsForceRunMode()) {
                $jobData['mode'] = Job::MODE_RUN; // forceRun is overrided by run mode
            }

            return $jobData;
        } catch (InvalidConfigurationException $e) {
            throw new ClientException('Invalid configuration: ' . $e->getMessage(), 0, $e);
        } catch (StorageClientException $e) {
            throw new ClientException('Cannot resolve job parameters: ' . $e->getMessage(), 0, $e);
        }
    }

    private function resolveTag(): string
    {
        if (!empty($this->jobData['tag'])) {
            return (string) $this->jobData['tag'];
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
        $componentsApi = $this->getComponentsApiClient(ClientWrapper::BRANCH_MAIN);
        $componentData = $componentsApi->getComponent($this->jobData['componentId']);
        if (!empty($componentData['data']['definition']['tag'])) {
            return $componentData['data']['definition']['tag'];
        } else {
            throw new ClientException(sprintf('The component "%s" is not runnable.', $this->jobData['componentId']));
        }
    }

    private function resolveVariables(): VariableValues
    {
        $variableValues = VariableValues::fromDataArray($this->jobData);
        if (!$variableValues->isEmpty()) {
            return $variableValues;
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
        if (!empty($this->jobData['backend'])) {
            $backend = Backend::fromDataArray($this->jobData['backend']);
            if (!$backend->isEmpty()) {
                return $backend;
            }
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
        if (isset($this->jobData['parallelism']) && ($this->jobData['parallelism'] !== null)) {
            return (string) $this->jobData['parallelism'];
        }
        if (isset($this->getConfigData()['runtime']['parallelism'])
            && $this->getConfigData()['runtime']['parallelism'] !== null) {
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
        return $configurationDefinition->processData($this->jobData['configData'] ?? []);
    }

    private function getConfiguration(): array
    {
        if ($this->configuration === null) {
            if (isset($this->jobData['configId']) &&
                $this->jobData['configId'] !== null &&
                $this->jobData['configId'] !== ''
            ) {
                $componentsApi = $this->getComponentsApiClient(
                    !empty($this->jobData['branchId']) ? (string) $this->jobData['branchId'] : null
                );
                $this->configuration = $componentsApi->getConfiguration(
                    $this->jobData['componentId'],
                    $this->jobData['configId']
                );

                if (!empty($this->configuration['isDisabled']) && !$this->resolveIsForceRunMode()) {
                    throw new ConfigurationDisabledException(sprintf(
                        'Configuration "%s" of component "%s" is disabled.',
                        $this->jobData['configId'],
                        $this->jobData['componentId']
                    ));
                }

                $configurationDefinition = new OverridesConfigurationDefinition();
                $this->configuration = $configurationDefinition->processData($this->configuration['configuration']);
            } else {
                $this->configuration = [];
            }
        }
        return $this->configuration;
    }

    private function getComponentsApiClient(?string $branchId): Components
    {
        return new Components(
            $this->storageClientFactory->getClientWrapper(
                $this->jobData['#tokenString'],
                $branchId
            )->getBranchClientIfAvailable()
        );
    }

    private function resolveIsForceRunMode(): bool
    {
        return isset($this->jobData['mode']) && $this->jobData['mode'] === Job::MODE_FORCE_RUN;
    }
}

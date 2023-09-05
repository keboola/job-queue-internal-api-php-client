<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\ConfigurationDisabledException;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Backend;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Executor;
use Keboola\PermissionChecker\BranchType;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

/**
 * @internal
 */
class JobRuntimeResolver
{
    private const JOB_TYPES_WITH_DEFAULT_BACKEND = [
        JobInterface::TYPE_STANDARD,
    ];

    private const PAY_AS_YOU_GO_FEATURE = 'pay-as-you-go';

    private StorageClientPlainFactory $storageClientFactory;
    private ?array $configuration;
    private array $componentData;
    private array $jobData;

    public function __construct(StorageClientPlainFactory $storageClientFactory)
    {
        $this->storageClientFactory = $storageClientFactory;
    }

    public function resolveJobData(array $jobData, array $tokenInfo): array
    {
        $this->configuration = null;
        $this->jobData = $jobData;

        try {
            $this->componentData = $this->getComponentsApiClient(null)
                ->getComponent($jobData['componentId']);
            $jobData['tag'] = $this->resolveTag($jobData);
            $variableValues = $this->resolveVariables();
            $jobData['parallelism'] = $this->resolveParallelism($jobData);
            $jobData['executor'] = $this->resolveExecutor($jobData)->value;
            $jobData['branchType'] = $this->resolveBranchType($jobData)->value;

            // set type after resolving parallelism
            $jobData['type'] = $this->resolveJobType($jobData);

            // set backend after resolving type
            $jobData['backend'] = $this->resolveBackend($jobData, $tokenInfo)->toDataArray();

            foreach ($variableValues->asDataArray() as $key => $value) {
                $jobData[$key] = $value;
            }
            return $jobData;
        } catch (InvalidConfigurationException $e) {
            throw new ClientException('Invalid configuration: ' . $e->getMessage(), 0, $e);
        } catch (StorageClientException $e) {
            throw new ClientException('Cannot resolve job parameters: ' . $e->getMessage(), 0, $e);
        }
    }

    private function resolveTag(array $jobData): string
    {
        if (!empty($jobData['tag'])) {
            return (string) $jobData['tag'];
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
        if (!empty($this->componentData['data']['definition']['tag'])) {
            return $this->componentData['data']['definition']['tag'];
        } else {
            throw new ClientException(sprintf('The component "%s" is not runnable.', $jobData['componentId']));
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

    private function getDefaultBackendContext(array $jobData, string $componentType): ?string
    {
        if (!in_array($jobData['type'], self::JOB_TYPES_WITH_DEFAULT_BACKEND)) {
            return null;
        }

        return sprintf(
            '%s-%s',
            $jobData['projectId'],
            $componentType,
        );
    }

    private function getBackendFromJobdata(array $jobData): Backend
    {
        return is_array($jobData['backend'])
            ? Backend::fromDataArray($jobData['backend']) : new Backend(null, null, null);
    }

    private function getBackendFromConfigData(): Backend
    {
        $configData = $this->getConfigData();
        return !empty($configData['runtime']['backend'])
            ? Backend::fromDataArray($configData['runtime']['backend']) : new Backend(null, null, null);
    }

    private function getBackendFromConfiguration(): Backend
    {
        $configuration = $this->getConfiguration();
        return !empty($configuration['runtime']['backend'])
            ? Backend::fromDataArray($configuration['runtime']['backend']) : new Backend(null, null, null);
    }

    private function mergeBackendsData(Backend $backendOne, Backend $backendTwo): Backend
    {
        return Backend::fromDataArray(array_merge(
            array_filter($backendOne->toDataArray()),
            array_filter($backendTwo->toDataArray()),
        ));
    }

    private function getBackend(array $jobData): Backend
    {
        $backend = new Backend(null, null, null);

        $overrideByBackend = $this->getBackendFromConfiguration();
        $backend = $this->mergeBackendsData($backend, $overrideByBackend);

        $overrideByBackend = $this->getBackendFromConfigData();
        $backend = $this->mergeBackendsData($backend, $overrideByBackend);

        $overrideByBackend = $this->getBackendFromJobdata($jobData);
        return $this->mergeBackendsData($backend, $overrideByBackend);
    }

    private function resolveBackend(array $jobData, array $tokenInfo): Backend
    {
        $tempBackend = $this->getBackend($jobData);

        if ($tempBackend->isEmpty()) {
            return new Backend(
                null,
                null,
                $this->getDefaultBackendContext($jobData, $this->componentData['type']),
            );
        }

        // decide whether to set "type' (aka workspaceSize) or containerType (aka containerSize)
        $stagingStorage = $this->componentData['data']['staging_storage']['input'] ?? '';
        $backendContext = $tempBackend->getContext() ?? $this->getDefaultBackendContext(
            $jobData,
            $this->componentData['type'],
        );

        /* Possible values of staging storage: https://github.com/keboola/docker-bundle/blob/ec9a628b614a70d0ed8a6ec36f2b6003a8e07ed4/src/Docker/Configuration/Component.php#L87
        For the purpose of setting backend, we consider: 'local', 's3', 'abs', 'none' to use container.
        For workspace size, we only consider 'workspace-snowflake' as it is the only backend supporting scaling.
        During this we ignore any containerType setting received in $tempBackend, which so far is intentional.
        We also ignore backend settings for other workspace types, as they do not make any sense at the moment.
        */
        if (in_array($stagingStorage, ['local', 's3', 'abs', 'none']) &&
            !in_array(self::PAY_AS_YOU_GO_FEATURE, $tokenInfo['owner']['features'] ?? [])
        ) {
            return new Backend(null, $tempBackend->getType(), $backendContext);
        }
        if ($stagingStorage === 'workspace-snowflake') {
            // dynamic workspace si hidden behind another feature `workspace-snowflake-dynamic-backend-size`
            // that is checked in SAPI, so we don't check it here, yet
            return new Backend($tempBackend->getType(), null, $backendContext);
        }
        return new Backend(null, null, $backendContext);
    }

    private function resolveParallelism(array $jobData): ?string
    {
        if (isset($jobData['parallelism']) && ($jobData['parallelism'] !== null)) {
            return (string) $jobData['parallelism'];
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
                    !empty($this->jobData['branchId']) ? (string) $this->jobData['branchId'] : null,
                );
                $this->configuration = $componentsApi->getConfiguration(
                    $this->jobData['componentId'],
                    $this->jobData['configId'],
                );

                if (!empty($this->configuration['isDisabled']) && !$this->resolveIsForceRunMode()) {
                    throw new ConfigurationDisabledException(sprintf(
                        'Configuration "%s" of component "%s" is disabled.',
                        $this->jobData['configId'],
                        $this->jobData['componentId'],
                    ));
                }

                $configurationDefinition = new OverridesConfigurationDefinition();
                $this->configuration = $configurationDefinition->processData(
                    $this->configuration['configuration'] ?? [],
                );
            } else {
                $this->configuration = [];
            }
        }
        return $this->configuration;
    }

    private function getComponentsApiClient(?string $branchId): Components
    {
        return new Components(
            $this->storageClientFactory->createClientWrapper(new ClientOptions(
                null,
                $this->jobData['#tokenString'],
                $branchId,
            ))->getBranchClient(),
        );
    }

    private function resolveIsForceRunMode(): bool
    {
        return isset($this->jobData['mode']) && $this->jobData['mode'] === JobInterface::MODE_FORCE_RUN;
    }

    private function resolveJobType(array $jobData): string
    {
        if (!empty($jobData['type'])) {
            return (string) $jobData['type'];
        }

        if ((intval($jobData['parallelism']) > 0) || $jobData['parallelism'] === JobInterface::PARALLELISM_INFINITY) {
            return JobInterface::TYPE_ROW_CONTAINER;
        } else {
            if ($jobData['componentId'] === JobFactory::ORCHESTRATOR_COMPONENT) {
                if (isset($jobData['configData']['phaseId']) && (string) ($jobData['configData']['phaseId']) !== '') {
                    return JobInterface::TYPE_PHASE_CONTAINER;
                } else {
                    return JobInterface::TYPE_ORCHESTRATION_CONTAINER;
                }
            }
        }
        return JobInterface::TYPE_STANDARD;
    }

    private function resolveExecutor(array $jobData): Executor
    {
        $value = $jobData['executor'] ??
            $this->getConfigData()['runtime']['executor'] ??
            $this->getConfiguration()['runtime']['executor'] ??
            Executor::getDefault()->value
        ;

        return Executor::from($value);
    }

    private function getBranchesApiClient(): DevBranches
    {
        return new DevBranches(
            $this->storageClientFactory->createClientWrapper(new ClientOptions(
                token: $this->jobData['#tokenString'],
            ))->getBasicClient(),
        );
    }

    public function resolveBranchType(array $jobData): BranchType
    {
        if ($jobData['branchId'] === 'default' || $jobData['branchId'] === null) {
            return BranchType::DEFAULT;
        }
        $branch = $this->getBranchesApiClient()->getBranch((int) $jobData['branchId']);
        return $branch['isDefault'] ? BranchType::DEFAULT : BranchType::DEV;
    }
}

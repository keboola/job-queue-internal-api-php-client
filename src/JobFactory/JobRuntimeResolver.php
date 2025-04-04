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
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use Keboola\StorageApiBranch\StorageApiToken;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

/**
 * @internal
 */
class JobRuntimeResolver
{
    private const JOB_TYPES_WITH_DEFAULT_BACKEND = [
        JobType::STANDARD->value,
    ];

    private const PAY_AS_YOU_GO_FEATURE = 'pay-as-you-go';
    private const NO_DIND_FEATURE = 'job-queue-no-dind';

    private ClientWrapper $clientWrapper;
    private Components $componentsApiClient;
    private ?array $configuration;
    private array $componentData;
    private array $jobData;

    public function __construct(
        private readonly StorageClientPlainFactory $storageClientFactory,
    ) {
    }

    public function resolveJobData(array $jobData, StorageApiToken $token): array
    {
        $this->configuration = null;
        $this->jobData = $jobData;

        try {
            $this->clientWrapper = $this->storageClientFactory->createClientWrapper(new ClientOptions(
                token: $jobData['#tokenString'],
                branchId: ((string) $jobData['branchId']) ?: null,
            ));

            $this->componentsApiClient = new Components($this->clientWrapper->getBranchClient());
            $this->componentData = $this->componentsApiClient->getComponent($jobData['componentId']);

            $jobData['tag'] = $this->resolveTag($jobData);
            $variableValues = $this->resolveVariables();
            $jobData['parallelism'] = $this->resolveParallelism($jobData);
            $jobData['executor'] = $this->resolveExecutor($jobData, $token)->value;
            $jobData = $this->resolveBranchType($jobData);

            // set type after resolving parallelism
            $jobData['type'] = $this->resolveJobType($jobData)->value;

            // set backend after resolving type
            $jobData['backend'] = $this->resolveBackend($jobData, $token)->toDataArray();

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

    private function resolveBackend(array $jobData, StorageApiToken $token): Backend
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
            !$token->hasFeature(self::PAY_AS_YOU_GO_FEATURE)
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
                $this->configuration = $this->componentsApiClient->getConfiguration(
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

    private function resolveIsForceRunMode(): bool
    {
        return isset($this->jobData['mode']) && $this->jobData['mode'] === JobInterface::MODE_FORCE_RUN;
    }

    private function resolveJobType(array $jobData): JobType
    {
        if (!empty($jobData['type'])) {
            return JobType::from((string) $jobData['type']);
        }

        if ((intval($jobData['parallelism']) > 0) || $jobData['parallelism'] === JobInterface::PARALLELISM_INFINITY) {
            return JobType::ROW_CONTAINER;
        } else {
            if ($jobData['componentId'] === JobFactory::FLOW_COMPONENT) {
                return JobType::ORCHESTRATION_CONTAINER;
            } elseif ($jobData['componentId'] === JobFactory::ORCHESTRATOR_COMPONENT) {
                if (isset($jobData['configData']['phaseId']) && (string) ($jobData['configData']['phaseId']) !== '') {
                    return JobType::PHASE_CONTAINER;
                } else {
                    return JobType::ORCHESTRATION_CONTAINER;
                }
            }
        }
        return JobType::STANDARD;
    }

    private function resolveExecutor(array $jobData, StorageApiToken $token): Executor
    {
        $value = $jobData['executor'] ??
            $this->getConfigData()['runtime']['executor'] ??
            $this->getConfiguration()['runtime']['executor'] ??
            ($token->hasFeature(self::NO_DIND_FEATURE) ? Executor::K8S_CONTAINERS->value : null) ??
            Executor::getDefault()->value
        ;

        return Executor::from($value);
    }

    public function resolveBranchType(array $jobData): array
    {
        $branchType = $this->clientWrapper->isDefaultBranch() ? BranchType::DEFAULT : BranchType::DEV;

        $jobData['branchType'] = $branchType->value;
        $jobData['branchId'] = $this->clientWrapper->getBranchId();

        return $jobData;
    }
}

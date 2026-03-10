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
 *
 * @phpstan-type JobDataInput array{
 *     id: string,
 *     runId: string,
 *     projectId: string,
 *     projectName?: string|null,
 *     tokenId: string,
 *     tokenDescription?: string|null,
 *     '#tokenString': string,
 *     componentId: string,
 *     configId?: string|int|null,
 *     mode: string|null,
 *     configRowIds?: list<scalar>|null,
 *     tag?: string|null,
 *     parentRunId?: string|null,
 *     configData?: array|null,
 *     createdTime?: string|null,
 *     startTime?: string|null,
 *     endTime?: string|null,
 *     delayedStartTime?: string|null,
 *     delay?: string|int|null,
 *     durationSeconds?: string|int|null,
 *     result?: array,
 *     usageData?: array,
 *     status: string,
 *     desiredStatus: string,
 *     type?: string|null,
 *     parallelism?: int|string|null,
 *     behavior?: array{onError?: string|null},
 *     isFinished?: bool,
 *     url?: string|null,
 *     branchId?: int|string|null,
 *     branchType?: string|null,
 *     variableValuesId?: string|null,
 *     variableValuesData?: array{values?: list<array{name: string, value: string}>},
 *     backend?: array{
 *         type?: string|null,
 *         containerType?: string|null,
 *         context?: string|null,
 *     }|null,
 *     executor?: string|null,
 *     metrics?: array{
 *         storage?: array{
 *             inputTablesBytesSum?: int|string|null,
 *             outputTablesBytesSum?: int|string|null,
 *         },
 *         backend?: array{
 *             size?: string|null,
 *             containerSize?: string|null,
 *             context?: string|null,
 *         }
 *     }|null,
 *     orchestrationJobId?: string|null,
 *     orchestrationTaskId?: string|null,
 *     orchestrationPhaseId?: string|null,
 *     onlyOrchestrationTaskIds?: list<scalar>|null,
 *     previousJobId?: string|null,
 *     runnerId?: string|null,
 *     deduplicationId?: string|null,
 * }
 *
 * @phpstan-type JobDataResolved array{
 *     id: string,
 *     runId: string,
 *     projectId: string,
 *     projectName?: string|null,
 *     tokenId: string,
 *     tokenDescription?: string|null,
 *     '#tokenString': string,
 *     componentId: string,
 *     configId?: string|int|null,
 *     mode: string|null,
 *     configRowIds?: list<scalar>,
 *     tag: string,
 *     parentRunId?: string|null,
 *     configData?: array,
 *     createdTime?: string|null,
 *     startTime?: string|null,
 *     endTime?: string|null,
 *     delayedStartTime?: string|null,
 *     delay?: string|int|null,
 *     durationSeconds?: string|int|null,
 *     result?: array,
 *     usageData?: array,
 *     status: string,
 *     desiredStatus: string,
 *     type: string,
 *     parallelism?: int|string|null,
 *     behavior?: array{onError?: string|null},
 *     isFinished?: bool,
 *     url?: string|null,
 *     branchId: string,
 *     branchType: string,
 *     variableValuesId?: string|null,
 *     variableValuesData?: array{values?: list<array{name: string, value: string}>},
 *     backend: array{
 *         type?: string|null,
 *         containerType?: string|null,
 *         context?: string|null,
 *     },
 *     executor: string|null,
 *     metrics?: array{
 *         storage?: array{
 *             inputTablesBytesSum?: int|string|null,
 *             outputTablesBytesSum?: int|string|null,
 *         },
 *         backend?: array{
 *             size?: string|null,
 *             containerSize?: string|null,
 *             context?: string|null,
 *         }
 *     },
 *     orchestrationJobId?: string|null,
 *     orchestrationTaskId?: string|null,
 *     orchestrationPhaseId?: string|null,
 *     onlyOrchestrationTaskIds?: list<scalar>,
 *     previousJobId?: string|null,
 *     runnerId?: string|null,
 *     deduplicationId?: string|null,
 * }
 */
class JobRuntimeResolver
{
    private const JOB_TYPES_WITH_DEFAULT_BACKEND = [
        JobType::STANDARD->value,
    ];

    private const PAY_AS_YOU_GO_FEATURE = 'pay-as-you-go';
    private const NO_DIND_FEATURE = 'job-queue-no-dind';
    private const PARALLELISM_ENFORCE_FEATURE = 'job-parallelism-enforce';

    private ClientWrapper $clientWrapper;
    private Components $componentsApiClient;
    private ?array $configuration;
    private ?array $rawConfiguration = null;
    /**
     * @var array{
     *     id: string,
     *     type: string,
     *     name: string,
     *     description: string,
     *     longDescription: string,
     *     version: int,
     *     complexity: string,
     *     categories: list<string>,
     *     data: array{
     *         definition: array{
     *             type: string,
     *             uri: string,
     *             tag?: string,
     *             digest?: string,
     *             repository?: array{
     *                 region?: string,
     *                 username?: string,
     *                 '#password'?: string,
     *                 server?: string,
     *             },
     *             build_options?: array<string, mixed>,
     *         },
     *         memory?: string,
     *         configuration_format?: string,
     *         process_timeout?: int,
     *         forward_token?: bool,
     *         forward_token_details?: bool,
     *         default_bucket?: bool,
     *         image_parameters?: mixed,
     *         network?: string,
     *         default_bucket_stage?: string,
     *         vendor?: mixed,
     *         synchronous_actions?: list<scalar>,
     *         logging?: array{
     *             type?: string,
     *             verbosity?: array<scalar>,
     *             gelf_server_type?: string,
     *             no_application_errors?: bool,
     *         },
     *         staging_storage?: array{
     *             input?: string,
     *             output?: string,
     *         },
     *     },
     * }
     */
    private array $componentData;
    /**
     * @var JobDataInput
     */
    private array $jobData;

    public function __construct(
        private readonly StorageClientPlainFactory $storageClientFactory,
    ) {
    }

    /**
     * @param JobDataInput $jobData
     * @param StorageApiToken $token
     * @return JobDataResolved
     */
    public function resolveJobData(array $jobData, StorageApiToken $token): array
    {
        $this->configuration = null;
        $this->rawConfiguration = null;
        $this->jobData = $jobData;

        try {
            $this->clientWrapper = $this->storageClientFactory->createClientWrapper(new ClientOptions(
                token: $jobData['#tokenString'],
                branchId: isset($jobData['branchId']) && $jobData['branchId'] !== '' ?
                    (string) $jobData['branchId'] :
                    null,
            ));

            $this->componentsApiClient = new Components($this->clientWrapper->getBranchClient());
            /** @var array{id: string, type: string, name: string, description: string, longDescription: string, version: int, complexity: string, categories: list<string>, data: array{definition: array{type: string, uri: string, tag?: string, digest?: string, repository?: array{region?: string, username?: string, '#password'?: string, server?: string}, build_options?: array<string, mixed>}, memory?: string, configuration_format?: string, process_timeout?: int, forward_token?: bool, forward_token_details?: bool, default_bucket?: bool, image_parameters?: mixed, network?: string, default_bucket_stage?: string, vendor?: mixed, synchronous_actions?: list<scalar>, logging?: array{type?: string, verbosity?: array<scalar>, gelf_server_type?: string, no_application_errors?: bool}, staging_storage?: array{input?: string, output?: string}}} $componentData */
            $componentData = $this->componentsApiClient->getComponent($jobData['componentId']);
            $this->componentData = $componentData;

            $jobData['tag'] = $this->resolveTag($jobData);
            $variableValues = $this->resolveVariables();
            $jobData['parallelism'] = $this->resolveParallelism($jobData, $token);
            $jobData['executor'] = $this->resolveExecutor($jobData, $token)->value;
            $jobData = $this->resolveBranchType($jobData);

            // set type after resolving parallelism
            /** @var array{type?: string|null, parallelism?: int|string|null, componentId: string, configData?: array{phaseId?: int|string|null}} $jobData */
            $jobData['type'] = $this->resolveJobType($jobData)->value;

            // set backend after resolving type
            /** @var array{projectId: string, componentId: string, type?: string|null, backend?: array{type?: string|null, containerType?: string|null, context?: string|null}} $jobData */
            $jobData['backend'] = $this->resolveBackend($jobData, $token)->toDataArray();

            foreach ($variableValues->asDataArray() as $key => $value) {
                $jobData[$key] = $value;
            }
            /** @var JobDataResolved $jobData */
            return $jobData;
        } catch (InvalidConfigurationException $e) {
            throw new ClientException('Invalid configuration: ' . $e->getMessage(), 0, $e);
        } catch (StorageClientException $e) {
            throw new ClientException('Cannot resolve job parameters: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * @param array{
     *     componentId: string,
     *     tag?: string|null,
     * } $jobData
     */
    private function resolveTag(array $jobData): string
    {
        if (!empty($jobData['tag'])) {
            return (string) $jobData['tag'];
        }
        if (!empty($this->getConfigData()['runtime']['tag'])) {
            return (string) $this->getConfigData()['runtime']['tag'];
        }
        $configuration = $this->getProcessedConfiguration();
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
        $configuration = $this->getProcessedConfiguration();
        // return these irrespective if they are empty, because if they are we'd create empty VariableValues anyway
        return VariableValues::fromDataArray($configuration);
    }

    /**
     * @param array{projectId: string, type?: string|null} $jobData
     */
    private function getDefaultBackendContext(array $jobData, string $componentType): ?string
    {
        if (!in_array($jobData['type'] ?? null, self::JOB_TYPES_WITH_DEFAULT_BACKEND)) {
            return null;
        }

        return sprintf(
            '%s-%s',
            $jobData['projectId'],
            $componentType,
        );
    }

    /**
     * @param array{backend?: array{type?: string|null, containerType?: string|null, context?: string|null}} $jobData
     */
    private function getBackendFromJobdata(array $jobData): Backend
    {
        return isset($jobData['backend'])
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
        $configuration = $this->getProcessedConfiguration();
        return !empty($configuration['runtime']['backend'])
            ? Backend::fromDataArray($configuration['runtime']['backend']) : new Backend(null, null, null);
    }

    private function mergeBackendsData(Backend $backendOne, Backend $backendTwo): Backend
    {
        /** @var array{type?: string|null, containerType?: string|null, context?: string|null} $mergedData */
        $mergedData = array_merge(
            array_filter($backendOne->toDataArray()),
            array_filter($backendTwo->toDataArray()),
        );
        return Backend::fromDataArray($mergedData);
    }

    /**
     * @param array{backend?: array{type?: string|null, containerType?: string|null, context?: string|null}} $jobData
     */
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

    /**
     * @param array{
     *     projectId: string,
     *     componentId: string,
     *     type?: string|null,
     *     backend?: array{type?: string|null, containerType?: string|null, context?: string|null}
     * } $jobData
     */
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

        Component "keboola.legacy-transformation" supports dynamic backend, but it doesn't have workspace staging.
        We can't set both types (workspaceSize and containerSize) at the same time, because it would assign
        more resources to the components pod. We only need workspaceSize for the legacy transformations.
        */
        if ($jobData['componentId'] === 'keboola.legacy-transformation') {
            return new Backend($tempBackend->getType(), null, $backendContext);
        }
        if (in_array($stagingStorage, ['local', 's3', 'abs', 'none']) &&
            !$token->hasFeature(self::PAY_AS_YOU_GO_FEATURE)
        ) {
            return new Backend(null, $tempBackend->getType(), $backendContext);
        }
        if ($stagingStorage === 'workspace-snowflake') {
            // Dynamic workspace si hidden behind another feature `workspace-snowflake-dynamic-backend-size`
            // that is checked in SAPI, so we don't check it here.
            return new Backend($tempBackend->getType(), null, $backendContext);
        }
        return new Backend(null, null, $backendContext);
    }

    /**
     * @param array{parallelism?: int|string|null} $jobData
     */
    private function resolveParallelism(array $jobData, StorageApiToken $token): ?string
    {
        $parallelism = null;

        if (isset($jobData['parallelism'])) {
            $parallelism = (string) $jobData['parallelism'];
        } elseif (isset($this->getConfigData()['runtime']['parallelism'])) {
            $parallelism = (string) $this->getConfigData()['runtime']['parallelism'];
        } else {
            $configuration = $this->getProcessedConfiguration();
            if (!empty($configuration['runtime']['parallelism'])) {
                $parallelism = (string) $configuration['runtime']['parallelism'];
            }
        }

        if (($parallelism === null || $parallelism === '0')
            && $token->hasFeature(self::PARALLELISM_ENFORCE_FEATURE)
        ) {
            return '1';
        }

        return $parallelism;
    }

    /**
     * @return array{
     *     variableValuesId?: string|null,
     *     variableValuesData?: array{values?: list<array{name: string, value: string}>},
     *     runtime?: array{
     *         tag?: string|null,
     *         image_tag?: string|null,
     *         process_timeout?: int|null,
     *         backend?: array{
     *             type?: string|null,
     *             context?: string|null,
     *         },
     *         executor?: string|null,
     *         parallelism?: int|string|null,
     *     },
     * }
     */
    private function getConfigData(): array
    {
        $configurationDefinition = new OverridesConfigurationDefinition();
        /** @var array{variableValuesId?: string|null, variableValuesData?: array{values?: list<array{name: string, value: string}>}, runtime?: array{tag?: string|null, image_tag?: string|null, process_timeout?: int|null, backend?: array{type?: string|null, context?: string|null}, executor?: string|null, parallelism?: int|string|null}} $result */
        $result = $configurationDefinition->processData($this->jobData['configData'] ?? []);
        return $result;
    }

    private function getRawConfiguration(): array
    {
        if ($this->rawConfiguration === null) {
            if (isset($this->jobData['configId']) &&
                $this->jobData['configId'] !== ''
            ) {
                $configuration = $this->componentsApiClient->getConfiguration(
                    $this->jobData['componentId'],
                    $this->jobData['configId'],
                );
                assert(is_array($configuration));
                $this->rawConfiguration = $configuration;

                if (!empty($configuration['isDisabled']) && !$this->resolveIsForceRunMode()) {
                    throw new ConfigurationDisabledException(sprintf(
                        'Configuration "%s" of component "%s" is disabled.',
                        $this->jobData['configId'],
                        $this->jobData['componentId'],
                    ));
                }
            } else {
                $this->rawConfiguration = [];
            }
        }

        return $this->rawConfiguration;
    }

    /**
     * @return array{
     *     variableValuesId?: string|null,
     *     variableValuesData?: array{values?: list<array{name: string, value: string}>},
     *     runtime?: array{
     *         tag?: string|null,
     *         image_tag?: string|null,
     *         process_timeout?: int|null,
     *         backend?: array{
     *             type?: string|null,
     *             context?: string|null,
     *         },
     *         executor?: string|null,
     *         parallelism?: int|string|null,
     *     },
     * }
     */
    private function getProcessedConfiguration(): array
    {
        if ($this->configuration === null) {
            $rawConfiguration = $this->getRawConfiguration();

            if (!empty($rawConfiguration)) {
                $configurationDefinition = new OverridesConfigurationDefinition();
                $this->configuration = $configurationDefinition->processData(
                    (array) ($rawConfiguration['configuration'] ?? []),
                );
            } else {
                $this->configuration = [];
            }
        }

        /** @var array{variableValuesId?: string|null, variableValuesData?: array{values?: list<array{name: string, value: string}>}, runtime?: array{tag?: string|null, image_tag?: string|null, process_timeout?: int|null, backend?: array{type?: string|null, context?: string|null}, executor?: string|null, parallelism?: int|string|null}} $result */
        $result = $this->configuration;
        return $result;
    }

    private function resolveIsForceRunMode(): bool
    {
        return isset($this->jobData['mode']) && $this->jobData['mode'] === JobInterface::MODE_FORCE_RUN;
    }

    /**
     * @param array{
     *     type?: string|null,
     *     parallelism?: int|string|null,
     *     componentId: string,
     *     configData?: array{phaseId?: string|int|null}
     * } $jobData
     */
    private function resolveJobType(array $jobData): JobType
    {
        if (!empty($jobData['type'])) {
            return JobType::from((string) $jobData['type']);
        }

        $parallelism = $jobData['parallelism'] ?? null;
        $hasParallelism = $parallelism === JobInterface::PARALLELISM_INFINITY || ((int) $parallelism) > 0;
        $configRows = (array) ($this->getRawConfiguration()['rows'] ?? []);
        if ($hasParallelism && count($configRows) >= 2) {
            return JobType::ROW_CONTAINER;
        }

        if ($jobData['componentId'] === JobFactory::FLOW_COMPONENT) {
            return JobType::ORCHESTRATION_CONTAINER;
        }

        if ($jobData['componentId'] === JobFactory::ORCHESTRATOR_COMPONENT) {
            $phaseId = (string) ($jobData['configData']['phaseId'] ?? '');
            return $phaseId !== '' ? JobType::PHASE_CONTAINER : JobType::ORCHESTRATION_CONTAINER;
        }

        return JobType::STANDARD;
    }

    /**
     * @param array{executor?: string|null} $jobData
     */
    private function resolveExecutor(array $jobData, StorageApiToken $token): Executor
    {
        $value = $jobData['executor'] ??
            $this->getConfigData()['runtime']['executor'] ??
            $this->getProcessedConfiguration()['runtime']['executor'] ??
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

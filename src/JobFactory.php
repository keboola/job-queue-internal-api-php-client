<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

abstract class JobFactory
{
    public const ORCHESTRATOR_COMPONENT = 'keboola.orchestrator';

    public const FLOW_COMPONENT = 'keboola.flow';

    public const PROTECTED_DEFAULT_BRANCH_FEATURE = 'protected-default-branch';

    /**
     * @param class-string<FullJobDefinition|NewJobDefinition> $validatorClass
     *
     * @return (
     *     array{
     *         id: string,
     *         runId: string,
     *         projectId: string,
     *         projectName?: string|null,
     *         tokenId: string,
     *         tokenDescription?: string|null,
     *         '#tokenString': string,
     *         componentId: string,
     *         configId?: string|int|null,
     *         mode: string|null,
     *         configRowIds?: list<scalar>,
     *         tag?: string|null,
     *         parentRunId?: string|null,
     *         configData?: array,
     *         createdTime?: string|null,
     *         startTime?: string|null,
     *         endTime?: string|null,
     *         delayedStartTime?: string|null,
     *         delay?: string|int|null,
     *         durationSeconds?: string|int|null,
     *         result?: array,
     *         usageData?: array,
     *         status: string,
     *         desiredStatus: string,
     *         type: string,
     *         parallelism?: int|string|null,
     *         behavior?: array{onError?: string|null},
     *         isFinished?: bool,
     *         url?: string|null,
     *         branchId?: string|null,
     *         branchType: string,
     *         variableValuesId?: string|null,
     *         variableValuesData?: array{values?: list<array{name: string, value: string}>},
     *         backend?: array{
     *             type?: string|null,
     *             containerType?: string|null,
     *             context?: string|null,
     *         },
     *         executor?: string|null,
     *         metrics?: array{
     *             storage?: array{
     *                 inputTablesBytesSum?: int|string|null,
     *                 outputTablesBytesSum?: int|string|null,
     *             },
     *             backend?: array{
     *                 size?: string|null,
     *                 containerSize?: string|null,
     *                 context?: string|null,
     *             }
     *         },
     *         orchestrationJobId?: string|null,
     *         orchestrationTaskId?: string|null,
     *         orchestrationPhaseId?: string|null,
     *         onlyOrchestrationTaskIds?: list<scalar>,
     *         previousJobId?: string|null,
     *         runnerId?: string|null,
     *         deduplicationId?: string|null,
     *     }
     *     |
     *     array{
     *         deduplicationId?: string|null,
     *         '#tokenString': string,
     *         configId?: string|int|null,
     *         componentId: string,
     *         result?: array,
     *         mode: string,
     *         configRowIds?: list<scalar>,
     *         tag?: string|null,
     *         parentRunId?: string|null,
     *         configData?: array,
     *         branchId?: string|int|null,
     *         delay?: string|int|null,
     *         delayedStartTime?: string|null,
     *         type?: string|null,
     *         parallelism?: int|string|null,
     *         variableValuesId?: string|null,
     *         variableValuesData?: array{values?: list<array{name: string, value: string}>},
     *         backend?: array{
     *             type?: string|null,
     *             containerType?: string|null,
     *             context?: string|null,
     *         },
     *         executor?: string|null,
     *         behavior?: array{onError?: string|null},
     *         orchestrationJobId?: string|null,
     *         orchestrationTaskId?: string|null,
     *         orchestrationPhaseId?: string|null,
     *         onlyOrchestrationTaskIds?: list<scalar>|null,
     *         previousJobId?: string|null,
     *     }
     * )
     */
    protected function validateJobData(array $data, string $validatorClass): array
    {
        try {
            // @phpstan-ignore-next-line
            return (new $validatorClass())->processData($data);
        } catch (InvalidConfigurationException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }
}

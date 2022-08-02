<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\JobFactory\FullJobDefinition;
use Keboola\JobQueueInternalClient\JobFactory\NewJobDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

abstract class JobFactory
{
    public const STATUS_CANCELLED = 'cancelled';
    public const STATUS_CREATED = 'created';
    public const STATUS_ERROR = 'error';
    public const STATUS_PROCESSING = 'processing';
    public const STATUS_SUCCESS = 'success';
    public const STATUS_TERMINATED = 'terminated';
    public const STATUS_TERMINATING = 'terminating';
    public const STATUS_WAITING = 'waiting';
    public const STATUS_WARNING = 'warning';

    public const DESIRED_STATUS_PROCESSING = 'processing';
    public const DESIRED_STATUS_TERMINATING = 'terminating';

    public const TYPE_STANDARD = 'standard';
    public const TYPE_ROW_CONTAINER = 'container';
    public const TYPE_PHASE_CONTAINER = 'phaseContainer';
    public const TYPE_ORCHESTRATION_CONTAINER = 'orchestrationContainer';

    public const PARALLELISM_INFINITY = 'infinity';
    public const ORCHESTRATOR_COMPONENT = 'keboola.orchestrator';

    public const PAY_AS_YOU_GO_FEATURE = 'pay-as-you-go';

    public static function getFinishedStatuses(): array
    {
        return [self::STATUS_SUCCESS, self::STATUS_WARNING, self::STATUS_ERROR, self::STATUS_CANCELLED,
            self::STATUS_TERMINATED];
    }

    public static function getAllStatuses(): array
    {
        return [self::STATUS_CANCELLED, self::STATUS_CREATED, self::STATUS_ERROR, self::STATUS_PROCESSING,
            self::STATUS_SUCCESS, self::STATUS_TERMINATED, self::STATUS_TERMINATING, self::STATUS_WAITING,
            self::STATUS_WARNING];
    }

    public static function getAllDesiredStatuses(): array
    {
        return [self::DESIRED_STATUS_PROCESSING, self::DESIRED_STATUS_TERMINATING];
    }

    public static function getKillableStatuses(): array
    {
        return [self::STATUS_CREATED, self::STATUS_WAITING, self::STATUS_PROCESSING];
    }

    public static function getAllowedJobTypes(): array
    {
        return [self::TYPE_STANDARD, self::TYPE_ROW_CONTAINER,
            self::TYPE_PHASE_CONTAINER, self::TYPE_ORCHESTRATION_CONTAINER,
        ];
    }

    public static function getAllowedParallelismValues(): array
    {
        $intValues = array_map(
            fn ($item) => (string) $item,
            range(0, 100)
        );
        return array_merge($intValues, ['infinity', null]);
    }

    /**
     * @param class-string<FullJobDefinition|NewJobDefinition> $validatorClass
     */
    protected function validateJobData(array $data, string $validatorClass): array
    {
        try {
            return (new $validatorClass())->processData($data);
        } catch (InvalidConfigurationException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }
}

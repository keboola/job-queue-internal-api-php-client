<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use DateTimeImmutable;
use Keboola\JobQueueInternalClient\Result\JobMetrics;

interface JobInterface
{
    public const RUN_ID_DELIMITER = '.';

    public const MODE_RUN = 'run';
    public const MODE_DEBUG = 'debug';
    public const MODE_FORCE_RUN = 'forceRun';

    public const STATUS_CANCELLED = 'cancelled';
    public const STATUS_CREATED = 'created';
    public const STATUS_ERROR = 'error';
    public const STATUS_PROCESSING = 'processing';
    public const STATUS_SUCCESS = 'success';
    public const STATUS_TERMINATED = 'terminated';
    public const STATUS_TERMINATING = 'terminating';
    public const STATUS_WAITING = 'waiting';
    public const STATUS_WARNING = 'warning';

    public const STATUSES_ALL = [
        self::STATUS_CANCELLED,
        self::STATUS_CREATED,
        self::STATUS_ERROR,
        self::STATUS_PROCESSING,
        self::STATUS_SUCCESS,
        self::STATUS_TERMINATED,
        self::STATUS_TERMINATING,
        self::STATUS_WAITING,
        self::STATUS_WARNING,
    ];

    public const STATUSES_FINISHED = [
        self::STATUS_SUCCESS,
        self::STATUS_WARNING,
        self::STATUS_ERROR,
        self::STATUS_CANCELLED,
        self::STATUS_TERMINATED,
    ];

    public const STATUSES_KILLABLE = [
        self::STATUS_CREATED,
        self::STATUS_WAITING,
        self::STATUS_PROCESSING,
    ];

    public const DESIRED_STATUS_PROCESSING = 'processing';
    public const DESIRED_STATUS_TERMINATING = 'terminating';

    public const DESIRED_STATUSES_ALL = [
        self::DESIRED_STATUS_PROCESSING,
        self::DESIRED_STATUS_TERMINATING,
    ];

    public const TYPE_STANDARD = 'standard';
    public const TYPE_ROW_CONTAINER = 'container';
    public const TYPE_PHASE_CONTAINER = 'phaseContainer';
    public const TYPE_ORCHESTRATION_CONTAINER = 'orchestrationContainer';

    public const TYPES_ALL = [
        self::TYPE_STANDARD,
        self::TYPE_ROW_CONTAINER,
        self::TYPE_PHASE_CONTAINER,
        self::TYPE_ORCHESTRATION_CONTAINER,
    ];

    public const PARALLELISM_INFINITY = 'infinity';
    public const PARALLELISM_ALL = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
        '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
        '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
        '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
        '41', '42', '43', '44', '45', '46', '47', '48', '49', '50',
        '51', '52', '53', '54', '55', '56', '57', '58', '59', '60',
        '61', '62', '63', '64', '65', '66', '67', '68', '69', '70',
        '71', '72', '73', '74', '75', '76', '77', '78', '79', '80',
        '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
        '91', '92', '93', '94', '95', '96', '97', '98', '99', '100',
        self::PARALLELISM_INFINITY, null,
    ];

    public function getId(): string;
    public function getComponentId(): string;
    public function getConfigData(): array;
    public function getConfigId(): ?string;
    public function getMode(): string;
    public function getProjectId(): string;
    public function getProjectName(): string;
    public function getDataPlaneId(): ?string;
    public function getResult(): array;
    public function getConfigRowIds(): array;
    public function getStatus(): string;
    public function getDesiredStatus(): string;
    public function getTag(): ?string;
    public function getTokenString(): string;
    public function getTokenId(): string;
    public function getTokenDescription(): string;
    public function getParentRunId(): string;
    public function getRunId(): string;
    public function isFinished(): bool;
    public function getUsageData(): array;
    public function getBackend(): Backend;
    public function getType(): string;
    public function getParallelism(): ?string;
    public function getBehavior(): Behavior;
    public function jsonSerialize(): array;
    public function getTokenDecrypted(): string;
    public function getComponentConfigurationDecrypted(): ?array;
    public function getConfigDataDecrypted(): array;
    public function getBranchId(): ?string;
    public function getVariableValuesId(): ?string;
    public function getVariableValuesData(): array;
    public function getVariableValues(): VariableValues;
    public function hasVariables(): bool;
    public function getStartTime(): ?DateTimeImmutable;
    public function getEndTime(): ?DateTimeImmutable;
    public function getDurationSeconds(): ?int;
    public function getMetrics(): JobMetrics;
    public function isInRunMode(): bool;
    public function getComponentSpecification(): ComponentSpecification;
    public function getComponentConfiguration(): array;
    public function getOrchestrationJobId(): ?string;
    public function getProjectFeatures(): array;
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory;

use DateTimeImmutable;
use InvalidArgumentException;
use Keboola\JobQueueInternalClient\JobFactory\Behavior;
use Keboola\JobQueueInternalClient\JobFactory\JobType;
use Keboola\JobQueueInternalClient\JobFactory\PlainJob;
use Keboola\JobQueueInternalClient\JobFactory\PlainJobInterface;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Backend;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Executor;
use Keboola\JobQueueInternalClient\JobFactory\VariableValues;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\PermissionChecker\BranchType;
use PHPUnit\Framework\TestCase;

class PlainJobTest extends TestCase
{
    private function createJobWithDefaults(array $jobData): PlainJob
    {
        return new PlainJob([
            'id' => 'job-123456456',
            'runId' => 'run-123456456',
            'configId' => 'config-454124290',
            'componentId' => 'keboola.ex-db-snowflake',
            'mode' => 'run',
            'configData' => [
                'parameters' => ['foo' => 'bar'],
            ],
            'status' => 'created',
            'desiredStatus' => 'processing',
            'projectId' => 'project-123',
            'projectName' => 'Test Project',
            'tokenId' => 'token-456',
            'tokenDescription' => 'My token',
            '#tokenString' => 'KBC::ProjectSecure::token',
            'branchId' => 'branch-987',
            'branchType' => BranchType::DEFAULT->value,
            'usageData' => [
                'storage' => [
                    'inputTablesBytesSum' => 567,
                    'outputTablesBytesSum' => 987,
                ],
            ],
            'type' => JobType::STANDARD->value,
            'orchestrationTaskId' => '123',
            'orchestrationPhaseId' => '456',
            'onlyOrchestrationTaskIds' => [['789']],
            'previousJobId' => '987',

            ...$jobData,
        ]);
    }

    public function testBranchTypeIsRequired(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Missing required parameter "branchType"');

        new PlainJob([]);
    }

    public function testDirectGetters(): void
    {
        $job = new PlainJob([
            'id' => 'job-id',
            'deduplicationId' => 'deduplication-id',
            'componentId' => 'component-id',
            'configData' => ['parameters' => ['foo' => 'bar']],
            'configId' => 'config-id',
            'mode' => 'run',
            'projectId' => 'project-id',
            'projectName' => 'project-name',
            'result' => ['bar' => 'foo'],
            'configRowIds' => ['config-row-id-1', 'config-row-id-2'],
            'status' => 'created',
            'desiredStatus' => 'processing',
            'tag' => 'latest',
            '#tokenString' => 'KBC::ProjectSecure::token',
            'tokenId' => 'token-id',
            'tokenDescription' => 'token-description',
            'runId' => 'run-id',
            'usageData' => ['storage' => ['inputTablesBytesSum' => 567, 'outputTablesBytesSum' => 987]],
            'backend' => [
                'type' => 'snowflake',
                'containerType' => 'small',
                'context' => 'foo',
            ],
            'executor' => 'k8sContainers',
            'type' => 'container',
            'parallelism' => '1',
            'behavior' => [
                'onError' => 'stop',
            ],
            'branchType' => 'default',
            'branchId' => 'branch-id',
            'variableValuesId' => 'variable-values-id',
            'variableValuesData' => ['var1' => 'val1'],
            'durationSeconds' => 123,
            'metrics' => [
                'storage' => ['inputTablesBytesSum' => 567, 'outputTablesBytesSum' => 987],
                'backend' => ['size' => 'xsmall', 'containerSize' => 'large', 'context' => 'ctx-foo'],
            ],
            'orchestrationJobId' => 'orchestration-job-id',
            'orchestrationTaskId' => 'orchestration-task-id',
            'orchestrationPhaseId' => 'orchestration-phase-id',
            'onlyOrchestrationTaskIds' => ['orchestration-task-id-1'],
            'previousJobId' => 'previous-job-id',
            'runnerId' => 'runner-id',
        ]);

        self::assertSame('job-id', $job->getId());
        self::assertSame('deduplication-id', $job->getDeduplicationId());
        self::assertSame('component-id', $job->getComponentId());
        self::assertSame(['parameters' => ['foo' => 'bar']], $job->getConfigData());
        self::assertSame('config-id', $job->getConfigId());
        self::assertSame('run', $job->getMode());
        self::assertSame('project-id', $job->getProjectId());
        self::assertSame('project-name', $job->getProjectName());
        self::assertSame(['bar' => 'foo'], $job->getResult());
        self::assertSame(['config-row-id-1', 'config-row-id-2'], $job->getConfigRowIds());
        self::assertSame('created', $job->getStatus());
        self::assertSame('processing', $job->getDesiredStatus());
        self::assertSame('latest', $job->getTag());
        self::assertSame('KBC::ProjectSecure::token', $job->getTokenString());
        self::assertSame('token-id', $job->getTokenId());
        self::assertSame('token-description', $job->getTokenDescription());
        self::assertSame('run-id', $job->getRunId());
        self::assertSame([
            'storage' => [
                'inputTablesBytesSum' => 567,
                'outputTablesBytesSum' => 987,
            ],
        ], $job->getUsageData());
        self::assertEquals(new Backend(
            type: 'snowflake',
            containerType: 'small',
            context: 'foo',
        ), $job->getBackend());
        self::assertSame(Executor::K8S_CONTAINERS, $job->getExecutor());
        self::assertSame(JobType::ROW_CONTAINER, $job->getType());
        self::assertSame('1', $job->getParallelism());
        self::assertEquals(new Behavior(onError: 'stop'), $job->getBehavior());
        self::assertSame(BranchType::DEFAULT, $job->getBranchType());
        self::assertSame('branch-id', $job->getBranchId());
        self::assertSame('variable-values-id', $job->getVariableValuesId());
        self::assertSame(['var1' => 'val1'], $job->getVariableValuesData());
        self::assertEquals(
            new VariableValues(variableValuesId: 'variable-values-id', variableValuesData: ['var1' => 'val1']),
            $job->getVariableValues(),
        );
        self::assertTrue($job->hasVariables());
        self::assertSame(123, $job->getDurationSeconds());
        self::assertEquals(
            JobMetrics::fromDataArray([
                'metrics' => [
                    'storage' => ['inputTablesBytesSum' => 567, 'outputTablesBytesSum' => 987],
                    'backend' => ['size' => 'xsmall', 'containerSize' => 'large', 'context' => 'ctx-foo'],
                ],
            ]),
            $job->getMetrics(),
        );
        self::assertSame('orchestration-job-id', $job->getOrchestrationJobId());
        self::assertSame('orchestration-task-id', $job->getOrchestrationTaskId());
        self::assertSame('orchestration-phase-id', $job->getOrchestrationPhaseId());
        self::assertSame(['orchestration-task-id-1'], $job->getOnlyOrchestrationTaskIds());
        self::assertSame('previous-job-id', $job->getPreviousJobId());
        self::assertSame('runner-id', $job->getRunnerId());
    }

    public function testDirectGettersDefaults(): void
    {
        $job = new PlainJob([
            'branchType' => 'default',
            'status' => 'created',
            'runId' => '123.456',
        ]);

        self::assertNull($job->getDeduplicationId());
        self::assertSame('', $job->getComponentId());
        self::assertSame([], $job->getConfigData());
        self::assertNull($job->getConfigId());
        self::assertSame([], $job->getResult());
        self::assertSame([], $job->getConfigRowIds());
        self::assertNull($job->getTag());
        self::assertSame([], $job->getUsageData());
        self::assertEquals(new Backend(null, null, null), $job->getBackend());
        self::assertSame(Executor::DIND, $job->getExecutor());
        self::assertSame(JobType::STANDARD, $job->getType());
        self::assertNull($job->getParallelism());
        self::assertNull($job->getBranchId());
        self::assertNull($job->getVariableValuesId());
        self::assertSame([], $job->getVariableValuesData());
        self::assertEquals(new VariableValues(null, []), $job->getVariableValues());
        self::assertFalse($job->hasVariables());
        self::assertNull($job->getDurationSeconds());
        self::assertEquals(JobMetrics::fromDataArray([]), $job->getMetrics());
        self::assertNull($job->getOrchestrationJobId());
        self::assertNull($job->getOrchestrationTaskId());
        self::assertNull($job->getOrchestrationPhaseId());
        self::assertNull($job->getOnlyOrchestrationTaskIds());
        self::assertNull($job->getPreviousJobId());
        self::assertNull($job->getRunnerId());
    }

    public static function provideFinishedStatuses(): iterable
    {
        yield 'cancelled' => [
            'status' => PlainJobInterface::STATUS_CANCELLED,
            'isFinished' => true,
        ];

        yield 'created' => [
            'status' => PlainJobInterface::STATUS_CREATED,
            'isFinished' => false,
        ];

        yield 'error' => [
            'status' => PlainJobInterface::STATUS_ERROR,
            'isFinished' => true,
        ];

        yield 'processing' => [
            'status' => PlainJobInterface::STATUS_PROCESSING,
            'isFinished' => false,
        ];

        yield 'success' => [
            'status' => PlainJobInterface::STATUS_SUCCESS,
            'isFinished' => true,
        ];

        yield 'terminated' => [
            'status' => PlainJobInterface::STATUS_TERMINATED,
            'isFinished' => true,
        ];

        yield 'terminating' => [
            'status' => PlainJobInterface::STATUS_TERMINATING,
            'isFinished' => false,
        ];

        yield 'waiting' => [
            'status' => PlainJobInterface::STATUS_WAITING,
            'isFinished' => false,
        ];

        yield 'warning' => [
            'status' => PlainJobInterface::STATUS_WARNING,
            'isFinished' => true,
        ];
    }

    /** @dataProvider provideFinishedStatuses */
    public function testIsFinished(string $status, bool $isFinished): void
    {
        $job = $this->createJobWithDefaults([
            'status' => $status,
        ]);
        self::assertSame($isFinished, $job->isFinished());
    }

    public function testGetParentRunId(): void
    {
        $job = $this->createJobWithDefaults([
            'runId' => '123.456',
        ]);

        self::assertSame('123', $job->getParentRunId());
    }

    public function provideStringDates(): iterable
    {
        yield 'ISO-8601' => [
            'inputValue' => '2021-01-01T01:02:03+04:00',
            'formattedValue' => '2021-01-01T01:02:03+04:00',
            'resultDate' => new DateTimeImmutable('2021-01-01T01:02:03+04:00'),
        ];

        yield 'date-time' => [
            'inputValue' => '2021-01-01 01:02:03',
            'formattedValue' => '2021-01-01T01:02:03+00:00',
            'resultDate' => new DateTimeImmutable('2021-01-01T01:02:03+00:00'),
        ];
    }

    /** @dataProvider provideStringDates */
    public function testGetStartTime(string $inputValue, string $formattedValue, DateTimeImmutable $result): void
    {
        $job = $this->createJobWithDefaults([
            'startTime' => $inputValue,
        ]);

        self::assertSame($formattedValue, $job->jsonSerialize()['startTime']);
        self::assertEquals($result, $job->getStartTime());
    }

    /** @dataProvider provideStringDates */
    public function testGetEndTime(string $inputValue, string $formattedValue, DateTimeImmutable $result): void
    {
        $job = $this->createJobWithDefaults([
            'endTime' => $inputValue,
        ]);

        self::assertSame($formattedValue, $job->jsonSerialize()['endTime']);
        self::assertEquals($result, $job->getEndTime());
    }

    /** @dataProvider provideStringDates */
    public function testGetCreatedTime(string $inputValue, string $formattedValue, DateTimeImmutable $result): void
    {
        $job = $this->createJobWithDefaults([
            'createdTime' => $inputValue,
        ]);

        self::assertSame($formattedValue, $job->jsonSerialize()['createdTime']);
        self::assertEquals($result, $job->getCreatedTime());
    }

    /** @dataProvider provideStringDates */
    public function testGetDelayedStartTime(string $inputValue, string $formattedValue, DateTimeImmutable $result): void
    {
        $job = $this->createJobWithDefaults([
            'delayedStartTime' => $inputValue,
        ]);

        self::assertSame($formattedValue, $job->jsonSerialize()['delayedStartTime']);
        self::assertEquals($result, $job->getDelayedStartTime());
    }

    public function testGetDelayedStartTimeFromDelay(): void
    {
        $job = $this->createJobWithDefaults([
            'delay' => 3600,
        ]);
        $delayedStartTime = new DateTimeImmutable('+ 3600 seconds');

        self::assertSame($delayedStartTime->format('c'), $job->jsonSerialize()['delayedStartTime']);
        self::assertEqualsWithDelta($delayedStartTime, $job->getDelayedStartTime(), 1);
    }
}

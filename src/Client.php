<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Closure;
use DateTime;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use JsonException;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Exception\ClientException as ApiClientException;
use Keboola\ApiClientBase\Json;
use Keboola\JobQueueInternalClient\Authenticator\InternalApiTokenAuthenticator;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\DeduplicationIdConflictException;
use Keboola\JobQueueInternalClient\Exception\StateTargetEqualsCurrentException;
use Keboola\JobQueueInternalClient\Exception\StateTerminalException;
use Keboola\JobQueueInternalClient\Exception\StateTransitionForbiddenException;
use Keboola\JobQueueInternalClient\JobFactory\PlainJobInterface;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\JobQueueInternalClient\Result\JobResult;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Validator\Constraints\NotBlank;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validation;
use Throwable;

/**
 * @template-covariant TJob of PlainJobInterface
 */
class Client
{
    private const DEFAULT_USER_AGENT = 'Internal PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 10;

    private readonly ApiClient $apiClient;
    private readonly LoggerInterface $logger;

    /**
     * @param ExistingJobFactoryInterface<TJob> $existingJobFactory
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        private readonly ExistingJobFactoryInterface $existingJobFactory,
        string $internalQueueApiUrl,
        ?string $internalQueueToken = null,
        ?string $storageApiToken = null,
        ?string $applicationToken = null,
        ?LoggerInterface $logger = null,
        int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        int $connectTimeout = ApiClientOptions::DEFAULT_CONNECT_TIMEOUT,
        int $requestTimeout = ApiClientOptions::DEFAULT_REQUEST_TIMEOUT,
        string $userAgent = self::DEFAULT_USER_AGENT,
        null|Closure|HandlerStack $requestHandler = null,
    ) {
        $this->logger = $logger ?? new NullLogger();

        $this->validateConfiguration(
            $internalQueueApiUrl,
            $internalQueueToken,
            $storageApiToken,
            $applicationToken,
        );

        assert($internalQueueApiUrl !== '');

        $this->apiClient = new ApiClient(
            $internalQueueApiUrl,
            $this->createAuthenticator($internalQueueToken, $storageApiToken, $applicationToken),
            new ApiClientOptions(
                userAgent: $userAgent,
                backoffMaxTries: $backoffMaxTries,
                connectTimeout: $connectTimeout,
                requestTimeout: $requestTimeout,
                requestHandler: $requestHandler,
                logger: $logger,
            ),
        );
    }

    private function validateConfiguration(
        string $internalQueueApiUrl,
        ?string $internalQueueToken,
        ?string $storageApiToken,
        ?string $applicationToken,
    ): void {
        $validator = Validation::createValidator();
        $errors = $validator->validate($internalQueueApiUrl, [new Url()]);

        $providedTokens = array_filter(
            [$internalQueueToken, $storageApiToken, $applicationToken],
            fn(?string $token) => $token !== null,
        );

        if (count($providedTokens) === 0) {
            throw new ClientException(
                'No token provided (internalQueueToken, storageApiToken and applicationToken are empty)',
            );
        }
        if (count($providedTokens) > 1) {
            throw new ClientException('More than one authentication token provided');
        }
        if ($internalQueueToken !== null) {
            $errors->addAll(
                $validator->validate($internalQueueToken, [new NotBlank()]),
            );
        }
        if ($storageApiToken !== null) {
            $errors->addAll(
                $validator->validate($storageApiToken, [new NotBlank()]),
            );
        }
        if ($applicationToken !== null) {
            $errors->addAll(
                $validator->validate($applicationToken, [new NotBlank()]),
            );
        }
        if ($errors->count() !== 0) {
            $messages = '';
            /** @var ConstraintViolationInterface $error */
            foreach ($errors as $error) {
                // @phpstan-ignore-next-line
                $messages .= 'Value "' . $error->getInvalidValue() . '" is invalid: ' . $error->getMessage() . "\n";
            }
            throw new ClientException('Invalid parameters when creating client: ' . $messages);
        }
    }

    public function addJobUsage(string $jobId, array $usage): void
    {
        // todo implement this
    }

    /**
     * @param PlainJobInterface $job
     * @return TJob
     */
    public function createJob(PlainJobInterface $job): PlainJobInterface
    {
        try {
            $jobData = json_encode($job, JSON_THROW_ON_ERROR);
            $request = $this->createRequest('POST', 'jobs', (string) $jobData);
        } catch (JsonException $e) {
            throw new ClientException('Invalid job data: ' . $e->getMessage(), $e->getCode(), $e);
        }
        $result = $this->sendRequest($request);
        return $this->existingJobFactory->loadFromExistingJobData($result);
    }

    public function createJobsBatch(array $jobs): array
    {
        try {
            $jobsData = json_encode($jobs, JSON_THROW_ON_ERROR);
            $request = $this->createRequest('POST', 'jobs/batch', (string) $jobsData);
        } catch (JsonException $e) {
            throw new ClientException('Invalid json of jobs: ' . $e->getMessage(), $e->getCode(), $e);
        }
        $result = $this->sendRequest($request);

        return array_map(function ($jobData) {
            assert(is_array($jobData));
            return $this->existingJobFactory->loadFromExistingJobData($jobData);
        }, $result);
    }

    /**
     * @return TJob
     */
    public function getJob(string $jobId): PlainJobInterface
    {
        if (empty($jobId)) {
            throw new ClientException(sprintf('Invalid job ID: "%s".', $jobId));
        }

        $request = $this->createRequest('GET', 'jobs/' . $jobId);
        $result = $this->sendRequest($request);
        return $this->existingJobFactory->loadFromExistingJobData($result);
    }

    /**
     * @return array<TJob>
     */
    public function listJobs(JobListOptions $listOptions, bool $fetchAllPages): array
    {
        $jobs = [];
        $i = 1;
        $listOptions = clone $listOptions;
        do {
            $request = $this->createRequest('GET', 'jobs?' . implode('&', $listOptions->getQueryParameters()));
            $result = $this->sendRequest($request);
            $chunk = $this->mapJobsFromResponse($result);
            $jobs = array_merge($jobs, $chunk);
            $listOptions->setOffset($i * $listOptions->getLimit());
            $i++;
        } while ($fetchAllPages && count($chunk) === $listOptions->getLimit());
        return $jobs;
    }

    /**
     * @return array<TJob>
     */
    public function getJobsWithIds(array $jobIds, ?JobsSortOptions $sortOptions = null): array
    {
        /* This is rather arbitrary size, we just need to make sure that the request is not too large. It would be
        better to measure the size of the request (depends on the id length), but that's a bit more complicated. */
        $chunkSize = 100;
        if (!$jobIds) {
            return [];
        }
        $chunks = array_chunk($jobIds, $chunkSize);
        $jobs = [];
        $listOptions = (new JobListOptions())->setLimit($chunkSize);
        if ($sortOptions) {
            $listOptions->setSortBy($sortOptions->getSortBy())->setSortOrder($sortOptions->getSortOrder());
        }
        foreach ($chunks as $chunk) {
            $listOptions->setIds($chunk);
            $jobs = array_merge($jobs, $this->listJobs($listOptions, false));
        }
        return $jobs;
    }

    public function getJobsWithStatus(array $statuses, ?JobsSortOptions $sortOptions = null): array
    {
        if (!$statuses) {
            return [];
        }
        $listOptions = (new JobListOptions())->setStatuses($statuses);
        if ($sortOptions) {
            $listOptions->setSortBy($sortOptions->getSortBy())->setSortOrder($sortOptions->getSortOrder());
        }
        return $this->listJobs($listOptions, true);
    }

    public function getJobsToStartWithStatus(array $statuses, ?JobsSortOptions $sortOptions = null): array
    {
        if (!$statuses) {
            return [];
        }
        $listOptions = (new JobListOptions())
            ->setStatuses($statuses)
            ->setDelayedStartTime(new DateTime());
        if ($sortOptions) {
            $listOptions->setSortBy($sortOptions->getSortBy())->setSortOrder($sortOptions->getSortOrder());
        }
        return $this->listJobs($listOptions, true);
    }

    /**
     * @return TJob
     */
    public function patchJob(string $jobId, JobPatchData $patchData): PlainJobInterface
    {
        if (empty($jobId)) {
            throw new ClientException(sprintf('Invalid job ID: "%s".', $jobId));
        }
        /** @noinspection PhpUnhandledExceptionInspection */
        $request = $this->createRequest(
            'PUT',
            'jobs/' . $jobId,
            (string) json_encode($patchData->jsonSerialize(), JSON_THROW_ON_ERROR),
        );
        return $this->existingJobFactory->loadFromExistingJobData($this->sendRequest($request));
    }

    /**
     * @return TJob
     */
    public function postJobResult(
        string $jobId,
        string $status,
        JobResult $result,
        ?JobMetrics $metrics = null,
    ): PlainJobInterface {
        if (empty($jobId)) {
            throw new ClientException(sprintf('Invalid job ID: "%s".', $jobId));
        }
        /** @noinspection PhpUnhandledExceptionInspection */
        $request = $this->createRequest(
            'PUT',
            'jobs/' . $jobId,
            (string) json_encode(
                ['status' => $status, 'result' => $result, 'metrics' => $metrics],
                JSON_THROW_ON_ERROR,
            ),
        );
        return $this->existingJobFactory->loadFromExistingJobData($this->sendRequest($request));
    }

    /**
     * @return TJob
     */
    public function patchJobResult(string $jobId, array $patchData): PlainJobInterface
    {
        if ($jobId === '') {
            throw new ClientException(sprintf('Invalid job ID: "%s".', $jobId));
        }
        $request = $this->createRequest(
            'PATCH',
            'jobs/' . $jobId . '/result',
            (string) json_encode($patchData, JSON_THROW_ON_ERROR),
        );
        return $this->existingJobFactory->loadFromExistingJobData($this->sendRequest($request));
    }

    /**
     * @return array<TJob>
     */
    public function listConfigurationsJobs(
        ListConfigurationsJobsOptions $options,
        bool $fetchFollowingPages = false,
    ): array {
        $jobs = [];
        $i = 1;
        $options = clone $options;
        do {
            $request = $this->createRequest(
                'GET',
                'configurations-jobs?' . implode('&', $options->getQueryParameters()),
            );
            $result = $this->sendRequest($request);
            $chunk = $this->mapJobsFromResponse($result);
            $jobs = array_merge($jobs, $chunk);
            $options->setOffset($i * $options->getLimit());
            $i++;
        } while ($fetchFollowingPages && count($chunk) === $options->getLimit());
        return $jobs;
    }

    /**
     * @return array<TJob>
     */
    public function listLatestConfigurationsJobs(
        ListLatestConfigurationsJobsOptions $options,
        bool $fetchFollowingPages = false,
    ): array {
        $jobs = [];
        $i = 1;
        $options = clone $options;
        do {
            $request = $this->createRequest(
                'GET',
                sprintf(
                    'latest-configurations-jobs?%s',
                    implode('&', $options->getQueryParameters()),
                ),
            );
            $result = $this->sendRequest($request);
            $chunk = $this->mapJobsFromResponse($result);
            $jobs = array_merge($jobs, $chunk);
            $options->setOffset($i * $options->getLimit());
            $i++;
        } while ($fetchFollowingPages && count($chunk) === $options->getLimit());
        return $jobs;
    }

    public function getJobsDurationSum(string $projectId): int
    {
        if (empty($projectId)) {
            throw new ClientException(sprintf('Invalid project ID: "%s".', $projectId));
        }

        $request = $this->createRequest('GET', 'stats/projects/' . $projectId);
        $response = $this->sendRequest($request);
        assert(isset($response['stats']) && is_array($response['stats']));
        assert(is_scalar($response['stats']['durationSum']));
        return (int) $response['stats']['durationSum'];
    }

    /**
     * @param non-empty-string|null $sortBy
     * @param "asc"|"desc"|null $sortOrder
     * @param int<0, max>|null $offset
     * @param int<1, 500>|null $limit
     * @return array<TJob>
     */
    public function searchJobs(
        ?SearchJobsFilters $filters = null,
        ?string $sortBy = null,
        ?string $sortOrder = null,
        ?int $offset = null,
        ?int $limit = null,
    ): array {
        $query = [];
        if ($filters !== null) {
            $query['filters'] = $filters->toQueryParams();
        }
        if ($sortBy !== null) {
            $query['sortBy'] = $sortBy;
        }
        if ($sortOrder !== null) {
            $query['sortOrder'] = $sortOrder;
        }
        if ($offset !== null) {
            $query['offset'] = $offset;
        }
        if ($limit !== null) {
            $query['limit'] = $limit;
        }

        return $this->searchJobsRaw($query);
    }

    /** @return TJob[] */
    public function searchJobsRaw(array $rawQuery): array
    {
        $request = $this->createRequest('GET', 'search/jobs?' . http_build_query($rawQuery));
        $response = $this->sendRequest($request);

        return $this->mapJobsFromSearchResponse($response);
    }

    /**
     * @param non-empty-string|null $sortBy
     * @param "asc"|"desc"|null $sortOrder
     * @return iterable<TJob>
     */
    public function searchAllJobs(
        ?SearchJobsFilters $filters = null,
        ?string $sortBy = null,
        ?string $sortOrder = null,
    ): iterable {
        $offset = 0;
        $limit = 100;

        do {
            $jobs = $this->searchJobs($filters, $sortBy, $sortOrder, $offset, $limit);
            foreach ($jobs as $job) {
                yield $job;
            }

            $offset += $limit;
        } while (count($jobs) === $limit);
    }

    /**
     * @param non-empty-array<non-empty-string> $groupBy
     * @param non-empty-string|null $sortBy
     * @param "asc"|"desc"|null $sortOrder
     * @param int<1, 500>|null $jobsPerGroup
     * @param int<1, 500>|null $limit
     * @return array<int, array{
     *     group: array<string, string>,
     *     jobs: TJob[]
     * }>
     */
    public function searchJobsGrouped(
        array $groupBy,
        ?SearchJobsFilters $filters = null,
        ?string $sortBy = null,
        ?string $sortOrder = null,
        ?int $jobsPerGroup = null,
        ?int $limit = null,
    ): array {
        $query = [
            'groupBy' => $groupBy,
        ];
        if ($filters !== null) {
            $query['filters'] = $filters->toQueryParams();
        }
        if ($sortBy !== null) {
            $query['sortBy'] = $sortBy;
        }
        if ($sortOrder !== null) {
            $query['sortOrder'] = $sortOrder;
        }
        if ($jobsPerGroup !== null) {
            $query['jobsPerGroup'] = $jobsPerGroup;
        }
        if ($limit !== null) {
            $query['limit'] = $limit;
        }

        return $this->searchJobsGroupedRaw($query);
    }

    /**
     * @return array<int, array{
     *     group: array<string, string>,
     *     jobs: TJob[]
     * }>
     */
    public function searchJobsGroupedRaw(array $query): array
    {
        $request = $this->createRequest('GET', 'search/grouped-jobs?' . http_build_query($query));
        /**
         * @var array<int, array{
         *     group: array<string, string>,
         *     jobs: TJob[]
         * }> $response
         */
        $response = $this->sendRequest($request);

        return array_map(
            function (array $group): array {
                $group['jobs'] = $this->mapJobsFromSearchResponse($group['jobs']);
                return $group;
            },
            $response,
        );
    }

    /** @return TJob[] */
    private function mapJobsFromSearchResponse(array $responseBody): array
    {
        return array_map(function ($jobData) {
            assert(is_array($jobData));
            return $this->existingJobFactory->loadFromElasticJobData($jobData);
        }, $responseBody);
    }

    /**
     * @return array<TJob>
     */
    private function mapJobsFromResponse(array $responseBody): array
    {
        $jobs = array_map(function ($jobData) {
            assert(is_array($jobData));
            try {
                return $this->existingJobFactory->loadFromExistingJobData($jobData);
            } catch (Throwable $e) {
                $this->logger->error('Failed to parse Job data: ' . $e->getMessage());
                // ignore invalid job
                return null;
            }
        }, $responseBody);
        return array_filter($jobs);
    }

    private function createAuthenticator(
        ?string $internalQueueToken,
        ?string $storageApiToken,
        ?string $applicationToken,
    ): RequestAuthenticatorInterface {
        if ($internalQueueToken !== null) {
            assert($internalQueueToken !== '');
            return new InternalApiTokenAuthenticator($internalQueueToken);
        }
        if ($storageApiToken !== null) {
            assert($storageApiToken !== '');
            return new StorageApiTokenAuthenticator($storageApiToken);
        }
        if ($applicationToken !== null) {
            assert($applicationToken !== '');
            return new ManageApiTokenAuthenticator($applicationToken);
        }
        // validateConfiguration() guarantees exactly one token is set; this satisfies the return type.
        throw new ClientException(
            'No token provided (internalQueueToken, storageApiToken and applicationToken are empty)',
        );
    }

    private function createRequest(string $method, string $uri, string $body = ''): Request
    {
        return new Request($method, $uri, ['Content-Type' => 'application/json'], $body);
    }

    private function sendRequest(Request $request): array
    {
        try {
            return $this->apiClient->sendRequestAndMapResponse($request, ArrayResponse::class)->data;
        } catch (ApiClientException $e) {
            $body = $this->decodeErrorBody($e->getResponseBody());
            $this->throwExceptionByStringCode($body, $e);
            throw new ClientException($e->getMessage(), $e->getCode(), $e, $body);
        }
    }

    /**
     * @return array<mixed>
     */
    private function decodeErrorBody(?string $body): array
    {
        if ($body === null || $body === '') {
            return [];
        }
        try {
            return Json::decodeArray($body);
        } catch (JsonException) {
            return [];
        }
    }

    private function throwExceptionByStringCode(array $body, Throwable $previous): void
    {
        if (!isset($body['context']) || !is_array($body['context']) || empty($body['context']['stringCode'])) {
            return;
        }

        switch ($body['context']['stringCode']) {
            case StateTargetEqualsCurrentException::STRING_CODE:
                throw new StateTargetEqualsCurrentException(
                    $previous->getMessage(),
                    $previous->getCode(),
                    $previous,
                    $body,
                );
            case StateTransitionForbiddenException::STRING_CODE:
                throw new StateTransitionForbiddenException(
                    $previous->getMessage(),
                    $previous->getCode(),
                    $previous,
                    $body,
                );
            case StateTerminalException::STRING_CODE:
                throw new StateTerminalException(
                    $previous->getMessage(),
                    $previous->getCode(),
                    $previous,
                    $body,
                );
            case DeduplicationIdConflictException::STRING_CODE:
                throw new DeduplicationIdConflictException(
                    $previous->getMessage(),
                    $previous->getCode(),
                    $previous,
                    $body,
                );
        }
    }
}

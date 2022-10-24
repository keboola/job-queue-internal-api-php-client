<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Closure;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\ClientException as GuzzleClientException;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use JsonException;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\StateTargetEqualsCurrentException;
use Keboola\JobQueueInternalClient\Exception\StateTerminalException;
use Keboola\JobQueueInternalClient\Exception\StateTransitionForbiddenException;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\Result\JobMetrics;
use Keboola\JobQueueInternalClient\Result\JobResult;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Validator\Constraints\NotBlank;
use Symfony\Component\Validator\Constraints\Range;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validation;
use Throwable;

class Client
{
    private const DEFAULT_USER_AGENT = 'Internal PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 10;
    private const JSON_DEPTH = 512;

    protected GuzzleClient $guzzle;
    private ExistingJobFactory $existingJobFactory;
    private LoggerInterface $logger;

    public function __construct(
        LoggerInterface $logger,
        ExistingJobFactory $existingJobFactory,
        string $internalQueueApiUrl,
        ?string $internalQueueToken,
        ?string $storageApiToken,
        array $options = []
    ) {
        $this->validateConfiguration($internalQueueApiUrl, $internalQueueToken, $storageApiToken, $options);
        if (!empty($options['backoffMaxTries'])) {
            $options['backoffMaxTries'] = intval($options['backoffMaxTries']);
        } else {
            $options['backoffMaxTries'] = self::DEFAULT_BACKOFF_RETRIES;
        }
        if (empty($options['userAgent'])) {
            $options['userAgent'] = self::DEFAULT_USER_AGENT;
        }

        $this->guzzle = $this->initClient($internalQueueApiUrl, $internalQueueToken, $storageApiToken, $options);
        $this->existingJobFactory = $existingJobFactory;
        $this->logger = $logger;
    }

    private function validateConfiguration(
        string $internalQueueApiUrl,
        ?string $internalQueueToken,
        ?string $storageApiToken,
        array $options
    ): void {
        $validator = Validation::createValidator();
        $errors = $validator->validate($internalQueueApiUrl, [new Url()]);
        if ($internalQueueToken === null && $storageApiToken === null) {
            throw new ClientException('Both InternalApiToken and StorageAPIToken are empty.');
        }
        if ($internalQueueToken !== null && $storageApiToken !== null) {
            throw new ClientException('Both InternalApiToken and StorageAPIToken are non-empty. Use only one.');
        }
        if ($internalQueueToken !== null) {
            $errors->addAll(
                $validator->validate($internalQueueToken, [new NotBlank()])
            );
        }
        if ($storageApiToken !== null) {
            $errors->addAll(
                $validator->validate($storageApiToken, [new NotBlank()])
            );
        }
        if (!empty($options['backoffMaxTries'])) {
            $errors->addAll($validator->validate($options['backoffMaxTries'], [new Range(['min' => 0, 'max' => 100])]));
        }
        if ($errors->count() !== 0) {
            $messages = '';
            /** @var ConstraintViolationInterface $error */
            foreach ($errors as $error) {
                $messages .= 'Value "' . $error->getInvalidValue() . '" is invalid: ' . $error->getMessage() . "\n";
            }
            throw new ClientException('Invalid parameters when creating client: ' . $messages);
        }
    }

    public function addJobUsage(string $jobId, array $usage): void
    {
        // todo implement this
    }

    public function createJob(JobInterface $job): JobInterface
    {
        try {
            $jobData = json_encode($job, JSON_THROW_ON_ERROR);
            $request = new Request('POST', 'jobs', [], $jobData);
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
            $request = new Request('POST', 'jobs/batch', [], $jobsData);
        } catch (JsonException $e) {
            throw new ClientException('Invalid json of jobs: ' . $e->getMessage(), $e->getCode(), $e);
        }
        $result = $this->sendRequest($request);

        return array_map(fn(array $jobData) => $this->existingJobFactory->loadFromExistingJobData($jobData), $result);
    }

    public function getJob(string $jobId): JobInterface
    {
        if (empty($jobId)) {
            throw new ClientException(sprintf('Invalid job ID: "%s".', $jobId));
        }

        $request = new Request('GET', 'jobs/' . $jobId);
        $result = $this->sendRequest($request);
        return $this->existingJobFactory->loadFromExistingJobData($result);
    }

    /**
     * @return array<JobInterface>
     */
    public function listJobs(JobListOptions $listOptions, bool $fetchAllPages): array
    {
        $jobs = [];
        $i = 1;
        $listOptions = clone $listOptions;
        do {
            $request = new Request('GET', 'jobs?' . implode('&', $listOptions->getQueryParameters()));
            $result = $this->sendRequest($request);
            $chunk = $this->mapJobsFromResponse($result);
            $jobs = array_merge($jobs, $chunk);
            $listOptions->setOffset($i * $listOptions->getLimit());
            $i++;
        } while ($fetchAllPages && count($chunk) === $listOptions->getLimit());
        return $jobs;
    }

    /**
     * @return array<JobInterface>
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

    public function patchJob(string $jobId, JobPatchData $patchData): JobInterface
    {
        if (empty($jobId)) {
            throw new ClientException(sprintf('Invalid job ID: "%s".', $jobId));
        }
        /** @noinspection PhpUnhandledExceptionInspection */
        $request = new Request(
            'PUT',
            'jobs/' . $jobId,
            [],
            json_encode($patchData->jsonSerialize(), JSON_THROW_ON_ERROR)
        );
        return $this->existingJobFactory->loadFromExistingJobData($this->sendRequest($request));
    }

    public function postJobResult(
        string $jobId,
        string $status,
        JobResult $result,
        ?JobMetrics $metrics = null
    ): JobInterface {
        if (empty($jobId)) {
            throw new ClientException(sprintf('Invalid job ID: "%s".', $jobId));
        }
        /** @noinspection PhpUnhandledExceptionInspection */
        $request = new Request(
            'PUT',
            'jobs/' . $jobId,
            [],
            json_encode(
                ['status' => $status, 'result' => $result, 'metrics' => $metrics],
                JSON_THROW_ON_ERROR
            )
        );
        return $this->existingJobFactory->loadFromExistingJobData($this->sendRequest($request));
    }

    /**
     * @return array<JobInterface>
     */
    public function listConfigurationsJobs(
        ListConfigurationsJobsOptions $options,
        bool $fetchFollowingPages = false
    ): array {
        $jobs = [];
        $i = 1;
        $options = clone $options;
        do {
            $request = new Request('GET', 'configurations-jobs?' . implode('&', $options->getQueryParameters()));
            $result = $this->sendRequest($request);
            $chunk = $this->mapJobsFromResponse($result);
            $jobs = array_merge($jobs, $chunk);
            $options->setOffset($i * $options->getLimit());
            $i++;
        } while ($fetchFollowingPages && count($chunk) === $options->getLimit());
        return $jobs;
    }

    /**
     * @return array<JobInterface>
     */
    public function listLatestConfigurationsJobs(
        ListLatestConfigurationsJobsOptions $options,
        bool $fetchFollowingPages = false
    ): array {
        $jobs = [];
        $i = 1;
        $options = clone $options;
        do {
            $request = new Request(
                'GET',
                sprintf(
                    'latest-configurations-jobs?%s',
                    implode('&', $options->getQueryParameters())
                )
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

        $request = new Request('GET', 'stats/projects/' . $projectId);
        $response = $this->sendRequest($request);
        return $response['stats']['durationSum'];
    }

    /**
     * @return array<JobInterface>
     */
    private function mapJobsFromResponse(array $responseBody): array
    {
        $jobs = array_map(function (array $jobData): ?JobInterface {
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

    private function createDefaultDecider(int $maxRetries): Closure
    {
        return function (
            int $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            $error = null
        ) use ($maxRetries) {
            if ($retries >= $maxRetries) {
                $this->logger->notice(sprintf('We have tried this %d times. Giving up.', $maxRetries));
                return false;
            } elseif ($response && $response->getStatusCode() >= 500) {
                $this->logger->notice(sprintf(
                    'Got a %s response for this reason: %s, retrying.',
                    $response->getStatusCode(),
                    $response->getReasonPhrase()
                ));
                return true;
            } elseif ($error) {
                if ($error instanceof Throwable) {
                    $this->logger->notice(sprintf(
                        'Got a %s error with this message: %s, retrying.',
                        $error->getCode(),
                        $error->getMessage()
                    ));
                }
                return true;
            } else {
                return false;
            }
        };
    }

    private function initClient(
        string $url,
        ?string $internalToken,
        ?string $storageToken,
        array $options = []
    ): GuzzleClient {
        // Initialize handlers (start with those supplied in constructor)
        if (isset($options['handler']) && is_callable($options['handler'])) {
            $handlerStack = HandlerStack::create($options['handler']);
        } else {
            $handlerStack = HandlerStack::create();
        }
        // Set exponential backoff
        $handlerStack->push(Middleware::retry($this->createDefaultDecider($options['backoffMaxTries'])));
        // Set handler to set default headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) use ($internalToken, $storageToken, $options) {
                $request = $request->withHeader('User-Agent', $options['userAgent'])
                        ->withHeader('Content-type', 'application/json');
                if ($internalToken !== null) {
                    $request = $request->withHeader('X-JobQueue-InternalApi-Token', $internalToken);
                }
                if ($storageToken !== null) {
                    $request = $request->withHeader('X-StorageApi-Token', $storageToken);
                }
                return $request;
            }
        ));
        // Set client logger
        if (isset($options['logger']) && $options['logger'] instanceof LoggerInterface) {
            $handlerStack->push(Middleware::log(
                $options['logger'],
                new MessageFormatter(
                    '{hostname} {req_header_User-Agent} - [{ts}] "{method} {resource} {protocol}/{version}"' .
                    ' {code} {res_header_Content-Length}'
                )
            ));
        }
        // finally create the instance
        return new GuzzleClient([
            'base_uri' => $url,
            'handler' => $handlerStack,
            'connect_timeout' => 10,
            'timeout' => 120,
        ]);
    }

    private function sendRequest(Request $request): array
    {
        try {
            $response = $this->guzzle->send($request);
            $data = $this->decodeRequestBody($response);
            return $data ?: [];
        } catch (GuzzleClientException $e) {
            $body = $this->decodeRequestBody($e->getResponse());
            $this->throwExceptionByStringCode($body, $e);
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    private function decodeRequestBody(ResponseInterface $response): array
    {
        try {
            return (array) json_decode(
                (string) $response->getBody(),
                true,
                self::JSON_DEPTH,
                JSON_THROW_ON_ERROR
            );
        } catch (Throwable $e) {
            throw new ClientException('Unable to parse response body into JSON: ' . $e->getMessage());
        }
    }

    private function throwExceptionByStringCode(array $body, Throwable $previous): void
    {
        if (empty($body['context']['stringCode'])) {
            return;
        }

        switch ($body['context']['stringCode']) {
            case StateTargetEqualsCurrentException::STRING_CODE:
                throw new StateTargetEqualsCurrentException(
                    $previous->getMessage(),
                    $previous->getCode(),
                    $previous
                );
            case StateTransitionForbiddenException::STRING_CODE:
                throw new StateTransitionForbiddenException(
                    $previous->getMessage(),
                    $previous->getCode(),
                    $previous
                );
            case StateTerminalException::STRING_CODE:
                throw new StateTerminalException(
                    $previous->getMessage(),
                    $previous->getCode(),
                    $previous
                );
        }
    }
}

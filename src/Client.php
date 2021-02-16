<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Closure;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
use JsonException;
use Keboola\JobQueueInternalClient\Exception\ClientException;
use Keboola\JobQueueInternalClient\Exception\StateTargetEqualsCurrentException;
use Keboola\JobQueueInternalClient\JobFactory\JobInterface;
use Keboola\JobQueueInternalClient\JobFactory\JobResult;
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

    /** @var GuzzleClient */
    protected $guzzle;

    /** @var JobFactory */
    private $jobFactory;

    /** @var LoggerInterface */
    private $logger;

    public function __construct(
        LoggerInterface $logger,
        JobFactory $jobFactory,
        string $internalQueueApiUrl,
        string $internalQueueToken,
        array $options = []
    ) {
        $validator = Validation::createValidator();
        $errors = $validator->validate($internalQueueApiUrl, [new Url()]);
        $errors->addAll(
            $validator->validate($internalQueueToken, [new NotBlank()])
        );
        if (!empty($options['backoffMaxTries'])) {
            $errors->addAll($validator->validate($options['backoffMaxTries'], [new Range(['min' => 0, 'max' => 100])]));
            $options['backoffMaxTries'] = intval($options['backoffMaxTries']);
        } else {
            $options['backoffMaxTries'] = self::DEFAULT_BACKOFF_RETRIES;
        }
        if (empty($options['userAgent'])) {
            $options['userAgent'] = self::DEFAULT_USER_AGENT;
        }
        if ($errors->count() !== 0) {
            $messages = '';
            /** @var ConstraintViolationInterface $error */
            foreach ($errors as $error) {
                $messages .= 'Value "' . $error->getInvalidValue() . '" is invalid: ' . $error->getMessage() . "\n";
            }
            throw new ClientException('Invalid parameters when creating client: ' . $messages);
        }
        $this->guzzle = $this->initClient($internalQueueApiUrl, $internalQueueToken, $options);
        $this->jobFactory = $jobFactory;
        $this->logger = $logger;
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
        return $this->jobFactory->loadFromExistingJobData($result);
    }

    public function getJobFactory(): JobFactory
    {
        return $this->jobFactory;
    }

    public function getJob(string $jobId): JobInterface
    {
        $request = new Request('GET', 'jobs/' . $jobId);
        $result = $this->sendRequest($request);
        return $this->jobFactory->loadFromExistingJobData($result);
    }

    public function listJobs(JobListOptions $listOptions, bool $fetchAllPages): array
    {
        $jobs = [];
        $i = 1;
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

    public function getJobsWithIds(array $jobIds): array
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
        foreach ($chunks as $chunk) {
            $listOptions->setIds($chunk);
            $jobs = array_merge($jobs, $this->listJobs($listOptions, false));
        }
        return $jobs;
    }

    public function getJobsWithStatus(array $statuses): array
    {
        if (!$statuses) {
            return [];
        }
        $listOptions = (new JobListOptions())->setStatuses($statuses);
        return $this->listJobs($listOptions, true);
    }

    public function updateJob(JobInterface $newJob): array
    {
        /** @noinspection PhpUnhandledExceptionInspection */
        $request = new Request(
            'PUT',
            'jobs/' . $newJob->getId(),
            [],
            json_encode(
                [
                    'status' => $newJob->getStatus(),
                    'desiredStatus' => $newJob->getDesiredStatus(),
                ],
                JSON_THROW_ON_ERROR
            )
        );
        return $this->sendRequest($request);
    }

    public function patchJob(string $jobId, array $patchData): array
    {
        /** @noinspection PhpUnhandledExceptionInspection */
        $request = new Request(
            'PUT',
            'jobs/' . $jobId,
            [],
            json_encode($patchData, JSON_THROW_ON_ERROR)
        );
        return $this->sendRequest($request);
    }

    public function postJobResult(string $jobId, string $status, JobResult $result): array
    {
        if (empty($jobId)) {
            throw new ClientException(sprintf('Invalid job ID: "%s".', $jobId));
        }
        /** @noinspection PhpUnhandledExceptionInspection */
        $request = new Request(
            'PUT',
            'jobs/' . $jobId,
            [],
            json_encode(
                ['status' => $status, 'result' => $result],
                JSON_THROW_ON_ERROR
            )
        );
        return $this->sendRequest($request);
    }

    private function mapJobsFromResponse(array $responseBody): array
    {
        $jobs = array_map(function (array $jobData): ?JobInterface {
            try {
                return $this->jobFactory->loadFromExistingJobData($jobData);
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
            $retries,
            RequestInterface $request,
            ?ResponseInterface $response = null,
            $error = null
        ) use ($maxRetries) {
            if ($retries >= $maxRetries) {
                return false;
            } elseif ($response && $response->getStatusCode() >= 500) {
                return true;
            } elseif ($error) {
                return true;
            } else {
                return false;
            }
        };
    }

    private function initClient(string $url, string $token, array $options = []): GuzzleClient
    {
        // Initialize handlers (start with those supplied in constructor)
        if (isset($options['handler']) && $options['handler'] instanceof HandlerStack) {
            $handlerStack = HandlerStack::create($options['handler']);
        } else {
            $handlerStack = HandlerStack::create();
        }
        // Set exponential backoff
        $handlerStack->push(Middleware::retry($this->createDefaultDecider($options['backoffMaxTries'])));
        // Set handler to set default headers
        $handlerStack->push(Middleware::mapRequest(
            function (RequestInterface $request) use ($token, $options) {
                return $request
                    ->withHeader('User-Agent', $options['userAgent'])
                    ->withHeader('X-InternalApi-Token', $token)
                    ->withHeader('Content-type', 'application/json');
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
        return new GuzzleClient(['base_uri' => $url, 'handler' => $handlerStack]);
    }

    private function sendRequest(Request $request): array
    {
        try {
            $response = $this->guzzle->send($request);
            $data = json_decode($response->getBody()->getContents(), true, self::JSON_DEPTH, JSON_THROW_ON_ERROR);
            return $data ?: [];
        } catch (\GuzzleHttp\Exception\ClientException $e) {
            try {
                $body = json_decode(
                    $e->getResponse()->getBody()->getContents(),
                    true,
                    self::JSON_DEPTH,
                    JSON_THROW_ON_ERROR
                );
            } catch (Throwable $e2) {
                throw new ClientException($e->getMessage(), $e->getCode(), $e2);
            }
            if (!empty($body['context']['stringCode']) &&
                ($body['context']['stringCode'] === 'statusTargetEqualsCurrent')
            ) {
                throw new StateTargetEqualsCurrentException($e->getMessage(), $e->getCode(), $e);
            }
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        } catch (JsonException $e) {
            throw new ClientException('Unable to parse response body into JSON: ' . $e->getMessage());
        }
    }
}

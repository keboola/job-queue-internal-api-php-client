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
use Keboola\JobQueueInternalClient\JobFactory\Job;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Validator\Constraints\NotBlank;
use Symfony\Component\Validator\Constraints\Range;
use Symfony\Component\Validator\Constraints\Url;
use Symfony\Component\Validator\ConstraintViolationInterface;
use Symfony\Component\Validator\Validation;

class Client
{
    private const DEFAULT_USER_AGENT = 'Internal PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 10;

    /** @var GuzzleClient */
    protected $guzzle;

    /** @var JobFactory */
    private $jobFactory;

    public function __construct(
        JobFactory $jobFactory,
        string $internalQueueApiUrl,
        string $internalQueueToken,
        array $options = []
    ) {
        $validator = Validation::createValidator();
        $errors = $validator->validate($internalQueueApiUrl, [new Url(['message' => 'Queue API URL is not valid.'])]);
        $errors->addAll(
            $validator->validate($internalQueueToken, [new NotBlank(['message' => 'Token is required.'])])
        );
        if (!empty($options['backoffMaxTries'])) {
            $errors->addAll($validator->validate($internalQueueToken, [new Range(['min' => 0, 'max' => 100])]));
            $options['backoffMaxTries'] = intval($options['backoffMaxTries']);
        } else {
            $options['backoffMaxTries'] = self::DEFAULT_BACKOFF_RETRIES;
        }
        if (!empty($options['userAgent'])) {
            $options['userAgent'] = (string) $options['userAgent'];
        } else {
            $options['userAgent'] = self::DEFAULT_USER_AGENT;
        }
        if ($errors->count() !== 0) {
            $messages = '';
            /** @var ConstraintViolationInterface $error */
            foreach ($errors as $error) {
                $messages .= $error->getMessage() . "\n";
            }
            throw new ClientException('Invalid parameters when creating client: ' . $messages);
        }
        $this->guzzle = $this->initClient($internalQueueApiUrl, $internalQueueToken, $options);
        $this->jobFactory = $jobFactory;
    }

    public function addJobUsage(string $jobId, array $usage): void
    {
    }

    public function createJob(Job $job): Job
    {
        try {
            $jobData = json_encode($job, JSON_THROW_ON_ERROR);
            $request = new Request('POST', 'jobs/', [], $jobData);
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

    public function getJob(string $jobId): Job
    {
        $request = new Request('GET', 'jobs/' . $jobId);
        $result = $this->sendRequest($request);
        if (!$result) {
            throw new ClientException(sprintf('Job "%s" not found.', $jobId));
        }
        return new Job($result);
    }

    public function getJobsWithIds(array $jobIds): array
    {
        $conditions = array_map(function (string $status): string {
            return 'id:' . $status;
        }, $jobIds);
        $query = '(' . implode(' OR ', $conditions) . ')';
        $request = new Request('GET', 'jobs/?query=' . $query);
        $result = $this->sendRequest($request);
        return $this->mapJobsFromResponse($result);
    }

    public function getJobsWithStatus(array $statuses): array
    {
        $conditions = array_map(function (string $status): string {
            return 'status:' . $status;
        }, $statuses);
        $query = '(' . implode(' OR ', $conditions) . ')';
        $request = new Request('GET', 'jobs/?query=' . $query);
        $result = $this->sendRequest($request);
        return $this->mapJobsFromResponse($result);
    }

    public function updateJob(Job $newJob): array
    {
        $request = new Request(
            'PUT',
            'jobs/' . $newJob->getId(),
            [],
            json_encode(
                ['status' => $newJob->getStatus()],
                JSON_THROW_ON_ERROR
            )
        );
        return $this->sendRequest($request);
    }

    public function postJobResult(string $jobId, string $status, array $result): array
    {
        $request = new Request(
            'POST',
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
        $jobs = array_map(function (array $jobData): ?Job {
            try {
                return $this->jobFactory->loadFromExistingJobData($jobData);
            } catch (\Throwable $e) {
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
            $data = json_decode($response->getBody()->getContents(), true, 512, JSON_THROW_ON_ERROR);
            return $data ?: [];
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        } catch (JsonException $e) {
            throw new ClientException('Unable to parse response body into JSON: ' . $e->getMessage());
        }
    }
}

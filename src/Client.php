<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Closure;
use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\GuzzleException;
use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\MessageFormatter;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Request;
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
    public const STATUS_ERROR = 'error';
    public const STATUS_SUCCESS = 'success';

    public function getNewJobIds(): array
    {
        return ['159'];
    }

    public function addJobUsage(string $jobId, array $usage): void
    {
    }

    private function getFakeJobs(): array
    {
        return [
            '123' => new Job([
                'id' => '123',
                'project' => [
                    'id' => 572,
                ],
                'token' => [
                    'id' => '27978',
                    'token' => 'KBC::ProjectSecure::eJwBXwGg/mE6Mjp7aTowO3M6MTM1OiLe9QIAMQMGoIhavPq6m6p36E0ZdItZfa5qZ5QJNIbtB+Wsa1iilPXvN/fRkF+btionU7cQNUv9Tb+bPdnEgmQS0ZEJd0tePNOatjgweYKM9l6MrrPIdVAO1JpnR0NpjvzUptUb7PLenGQmp1m0xtts9ejToLI3t34gGVQZx/2kgU+B4h10tC4iO2k6MTtzOjE4NDoiAQIDAHiUapM47LpNvMSjuEzEf1BZ03rH6yxNXGD7eyDrPYUBcwGMnGFV4uchhbvPz6JZR6+IAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMRdbioyrD7L/Us5C1AgEQgDu9lTbzv9bPpNKtQOsUsXzac9JNzzlt4wFTkNeTLFICnQEaNIdN8Hck/JlC3z19TUgqMB3NDtbulmu/YCI7fVzGmd8=',
                ],
                'params' => [
                    'config' => '454124290',
                    'component' => 'keboola.ex-db-snowflake',
                    'mode' => 'run',
                ],
                'status' => 'waiting',
            ]),
            '456' => new Job([
                'id' => '456',
                'project' => [
                    'id' => 572,
                ],
                'token' => [
                    'id' => '27978',
                    'token' => 'KBC::ProjectSecure::eJwBXwGg/mE6Mjp7aTowO3M6MTM1OiLe9QIAMQMGoIhavPq6m6p36E0ZdItZfa5qZ5QJNIbtB+Wsa1iilPXvN/fRkF+btionU7cQNUv9Tb+bPdnEgmQS0ZEJd0tePNOatjgweYKM9l6MrrPIdVAO1JpnR0NpjvzUptUb7PLenGQmp1m0xtts9ejToLI3t34gGVQZx/2kgU+B4h10tC4iO2k6MTtzOjE4NDoiAQIDAHiUapM47LpNvMSjuEzEf1BZ03rH6yxNXGD7eyDrPYUBcwGMnGFV4uchhbvPz6JZR6+IAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMRdbioyrD7L/Us5C1AgEQgDu9lTbzv9bPpNKtQOsUsXzac9JNzzlt4wFTkNeTLFICnQEaNIdN8Hck/JlC3z19TUgqMB3NDtbulmu/YCI7fVzGmd8=',
                ],
                'params' => [
                    'config' => '454124290',
                    'component' => 'keboola.ex-db-snowflake',
                    'mode' => 'run',
                    'configData' => [
                        'parameters' => [
                            'db' => [
                                'port' => 443,
                                'ssh' => [
                                    'sshPort' => 22,
                                ],
                                'host' => 'kebooladev.snowflakecomputing.com',
                                'user' => 'HELP_TUTORIAL',
                                '#password' => 'KBC::ProjectSecure::eJwBOAHH/mE6Mjp7aTowO3M6OTc6It71AgAM/WBADvxCoaxFTMVP0M1mbi3h3qXcsxcZNiMQbqJ+1TSGyhbWPvaSPeUm1nsK2ghsN+umPR/5HoqeYIWOxZKQfZ/h0EZsOpV0B46sAy71eHlNUsqvYXmUalMdzW4iO2k6MTtzOjE4NDoiAQIDAHiUapM47LpNvMSjuEzEf1BZ03rH6yxNXGD7eyDrPYUBcwGMnGFV4uchhbvPz6JZR6+IAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMRdbioyrD7L/Us5C1AgEQgDu9lTbzv9bPpNKtQOsUsXzac9JNzzlt4wFTkNeTLFICnQEaNIdN8Hck/JlC3z19TUgqMB3NDtbulmu/YCI7fXDjgqo=',
                                'database' => 'HELP_TUTORIAL',
                                'schema' => 'HELP_TUTORIAL',
                                'warehouse' => 'DEV',
                            ],
                            'tables' => [
                                [
                                    'outputTable' => 'in.c-keboola-ex-db-snowflake-454124290.user',
                                    'columns' => [],
                                    'name' => 'USER',
                                    'incremental' => false,
                                    'id' => 85768,
                                    'enabled' => true,
                                    'table' => [
                                        'schema' => 'HELP_TUTORIAL',
                                        'tableName' => 'USER',
                                    ],
                                    'primaryKey' => [],
                                ],
                            ],
                        ],
                    ],
                ],
                'status' => 'waiting',
            ]),
            '789' => new Job([
                'id' => '789',
                'project' => [
                    'id' => 572,
                ],
                'token' => [
                    'id' => '27978',
                    'token' => 'KBC::ProjectSecure::eJwBXwGg/mE6Mjp7aTowO3M6MTM1OiLe9QIAMQMGoIhavPq6m6p36E0ZdItZfa5qZ5QJNIbtB+Wsa1iilPXvN/fRkF+btionU7cQNUv9Tb+bPdnEgmQS0ZEJd0tePNOatjgweYKM9l6MrrPIdVAO1JpnR0NpjvzUptUb7PLenGQmp1m0xtts9ejToLI3t34gGVQZx/2kgU+B4h10tC4iO2k6MTtzOjE4NDoiAQIDAHiUapM47LpNvMSjuEzEf1BZ03rH6yxNXGD7eyDrPYUBcwGMnGFV4uchhbvPz6JZR6+IAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMRdbioyrD7L/Us5C1AgEQgDu9lTbzv9bPpNKtQOsUsXzac9JNzzlt4wFTkNeTLFICnQEaNIdN8Hck/JlC3z19TUgqMB3NDtbulmu/YCI7fVzGmd8=',
                ],
                'params' => [
                    'config' => '470523946',
                    'component' => 'keboola.ex-http',
                    'mode' => 'run',
                    'row' => '470523979',
                ],
                'status' => 'waiting',
            ]),
            '13579' => new Job([
                'id' => '13579',
                'project' => [
                    'id' => 572,
                ],
                'token' => [
                    'id' => '27978',
                    'token' => 'KBC::ProjectSecure::eJwBXwGg/mE6Mjp7aTowO3M6MTM1OiLe9QIAMQMGoIhavPq6m6p36E0ZdItZfa5qZ5QJNIbtB+Wsa1iilPXvN/fRkF+btionU7cQNUv9Tb+bPdnEgmQS0ZEJd0tePNOatjgweYKM9l6MrrPIdVAO1JpnR0NpjvzUptUb7PLenGQmp1m0xtts9ejToLI3t34gGVQZx/2kgU+B4h10tC4iO2k6MTtzOjE4NDoiAQIDAHiUapM47LpNvMSjuEzEf1BZ03rH6yxNXGD7eyDrPYUBcwGMnGFV4uchhbvPz6JZR6+IAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMRdbioyrD7L/Us5C1AgEQgDu9lTbzv9bPpNKtQOsUsXzac9JNzzlt4wFTkNeTLFICnQEaNIdN8Hck/JlC3z19TUgqMB3NDtbulmu/YCI7fVzGmd8=',
                ],
                'params' => [
                    'config' => '489371184',
                    'component' => 'keboola.app-end-of-life',
                    'mode' => 'run',
                ],
                'status' => 'waiting',
            ]),
            '24680' => new Job([
                'id' => '24680',
                'project' => [
                    'id' => 572,
                ],
                'token' => [
                    'id' => '27978',
                    'token' => 'KBC::ProjectSecure::eJwBXwGg/mE6Mjp7aTowO3M6MTM1OiLe9QIAMQMGoIhavPq6m6p36E0ZdItZfa5qZ5QJNIbtB+Wsa1iilPXvN/fRkF+btionU7cQNUv9Tb+bPdnEgmQS0ZEJd0tePNOatjgweYKM9l6MrrPIdVAO1JpnR0NpjvzUptUb7PLenGQmp1m0xtts9ejToLI3t34gGVQZx/2kgU+B4h10tC4iO2k6MTtzOjE4NDoiAQIDAHiUapM47LpNvMSjuEzEf1BZ03rH6yxNXGD7eyDrPYUBcwGMnGFV4uchhbvPz6JZR6+IAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMRdbioyrD7L/Us5C1AgEQgDu9lTbzv9bPpNKtQOsUsXzac9JNzzlt4wFTkNeTLFICnQEaNIdN8Hck/JlC3z19TUgqMB3NDtbulmu/YCI7fVzGmd8=',
                ],
                'params' => [
                    'config' => '463211215',
                    'component' => 'keboola.wr-slack',
                    'mode' => 'run',
                ],
                'status' => 'waiting',
            ]),
            '159' => new Job([
                'id' => '159',
                'project' => [
                    'id' => 572,
                ],
                'token' => [
                    'id' => '27978',
                    'token' => 'KBC::ProjectSecure::eJwBXwGg/mE6Mjp7aTowO3M6MTM1OiLe9QIAMQMGoIhavPq6m6p36E0ZdItZfa5qZ5QJNIbtB+Wsa1iilPXvN/fRkF+btionU7cQNUv9Tb+bPdnEgmQS0ZEJd0tePNOatjgweYKM9l6MrrPIdVAO1JpnR0NpjvzUptUb7PLenGQmp1m0xtts9ejToLI3t34gGVQZx/2kgU+B4h10tC4iO2k6MTtzOjE4NDoiAQIDAHiUapM47LpNvMSjuEzEf1BZ03rH6yxNXGD7eyDrPYUBcwGMnGFV4uchhbvPz6JZR6+IAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMRdbioyrD7L/Us5C1AgEQgDu9lTbzv9bPpNKtQOsUsXzac9JNzzlt4wFTkNeTLFICnQEaNIdN8Hck/JlC3z19TUgqMB3NDtbulmu/YCI7fVzGmd8=',
                ],
                'params' => [
                    'component' => 'keboola.r-transformation',
                    'mode' => 'run',
                    'configData' => [
                        'parameters' => [
                            'script' => [
                                'app$logInfo(R.Version())',
                            ],
                        ],
                    ],
                ],
                'status' => 'waiting',
            ]),
        ];
    }

    public function getFakeJobData(array $jobIds): array
    {
        $jobs = array_filter(
            $this->getFakeJobs(),
            function ($key) use ($jobIds) {
                return in_array($key, $jobIds);
            },
            ARRAY_FILTER_USE_KEY
        );
        return array_values($jobs);
    }

    /**
     * @var GuzzleClient
     */
    protected $guzzle;

    private const DEFAULT_USER_AGENT = 'Internal PHP Client';
    private const DEFAULT_BACKOFF_RETRIES = 10;

    public function __construct(string $url, string $token, array $options = [])
    {
        $validator = Validation::createValidator();
        $errors = $validator->validate($url, [new Url([])]);
        $errors->addAll($validator->validate($token, [new NotBlank([])]));
        if (!empty($options['backoffMaxTries'])) {
            $errors->addAll($validator->validate($token, [new Range(['min' => 0, 'max' => 100])]));
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
        $this->guzzle = $this->initClient($url, $token, $options);
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
        $handlerStack->push(Middleware::retry(self::createDefaultDecider($options['backoffMaxTries'])));
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
        return new GuzzleClient(['base_url' => $url, 'handler' => $handlerStack]);
    }

    private static function createDefaultDecider(int $maxRetries): Closure
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

    private function sendRequest(Request $request): array
    {
        try {
            $response = $this->guzzle->send($request);
            $data = json_decode($response->getBody()->getContents(), true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new ClientException('Unable to parse response body into JSON: ' . json_last_error());
            }
            return $data ?: [];
        } catch (GuzzleException $e) {
            throw new ClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    public function getJob(string $jobId): Job
    {
        return $this->getFakeJobs()[$jobId];

        $request = new Request('GET', 'jobs/' . $jobId);
        $result = $this->sendRequest($request);
        if (!$result) {
            throw new ClientException(sprintf('Job "%s" not found.', $jobId));
        }
    }

    public function postJobResult(string $jobId, string $status, array $result): array
    {
        return [];
        $request = new Request('POST', 'jobs/' . $jobId, [], json_encode(['status' => $status, 'result' => $result]));
        return $this->sendRequest($request);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\ApiClientBase\ErrorMessageResolverInterface;

/**
 * Never extracts a message from the response body, so {@see \Keboola\ApiClientBase\ApiClient}
 * falls back to the underlying transport (Guzzle) exception message.
 *
 * This preserves the historical {@see Exception\ClientException} message format — e.g.
 * "Client error: `GET .../jobs/123` resulted in a `404 Not Found` response: {...}" — that
 * callers (and the public-api controllers surfacing it) have always relied on. The default
 * resolver would instead surface the decoded `error`/`message` field, silently changing the
 * message and the API error responses built from it.
 */
final class TransportErrorMessageResolver implements ErrorMessageResolverInterface
{
    public function __invoke(string $responseBody, int $statusCode): ?string
    {
        return null;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\ApiClientBase\ResponseModelInterface;

/**
 * Minimal response model that carries the decoded response body as-is.
 *
 * The base ApiClient only returns void or a {@see ResponseModelInterface}; this shim lets the
 * Client keep mapping raw decoded arrays through its own job factory (paging, grouping, filtering).
 */
final class ArrayResponse implements ResponseModelInterface
{
    /**
     * @param array<mixed> $data
     */
    public function __construct(
        public readonly array $data,
    ) {
    }

    /**
     * @param array<mixed> $data
     */
    public static function fromResponseData(array $data): static
    {
        return new self($data);
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use Keboola\JobQueueInternalClient\Exception\ClientException;

class JobsSortOptions
{
    private string $sortBy;
    private string $sortOrder;

    /** @var string */
    public const SORT_ORDER_ASC = JobListOptions::SORT_ORDER_ASC;

    /** @var string */
    public const SORT_ORDER_DESC = JobListOptions::SORT_ORDER_DESC;

    public const SORT_ORDER_ALLOWED_VALUES = [
        JobListOptions::SORT_ORDER_ASC,
        JobListOptions::SORT_ORDER_DESC,
    ];

    public function getSortBy(): string
    {
        return $this->sortBy;
    }

    public function setSortBy(string $value): JobsSortOptions
    {
        $this->sortBy = $value;
        return $this;
    }

    public function getSortOrder(): string
    {
        return $this->sortOrder;
    }

    public function setSortOrder(string $value): JobsSortOptions
    {
        if (!in_array($value, self::SORT_ORDER_ALLOWED_VALUES)) {
            throw new ClientException(sprintf(
                'Allowed values for "sortOrder" are [%s].',
                implode(', ', self::SORT_ORDER_ALLOWED_VALUES),
            ));
        }
        $this->sortOrder = $value;
        return $this;
    }
}

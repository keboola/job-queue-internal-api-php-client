<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient;

use DateTimeImmutable;

class SearchJobsFilters
{
    public function __construct(
        /** @var non-empty-array<int<1, max>> */
        public ?array $id = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $runId = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $branchId = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $configId = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $configRowIds = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $projectId = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $tokenId = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $tokenDescription = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $componentId = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $status = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $desiredStatus = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $mode = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $tag = null,
        public ?DateTimeImmutable $startTimeFrom = null,
        public ?DateTimeImmutable $startTimeTo = null,
        public ?DateTimeImmutable $createdTimeFrom = null,
        public ?DateTimeImmutable $createdTimeTo = null,
        public ?DateTimeImmutable $endTimeFrom = null,
        public ?DateTimeImmutable $endTimeTo = null,
        /** @var null|positive-int */
        public ?int $durationSecondsFrom = null,
        /** @var null|positive-int */
        public ?int $durationSecondsTo = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $variableValuesId = null,
        /** @var non-empty-array<string> */
        public ?array $parentRunId = null,
        /** @var non-empty-array<non-empty-string> */
        public ?array $type = null,
    ) {
    }

    public function toQueryParams(): array
    {
        $query = (array) $this;

        foreach ([
            'startTimeFrom',
            'startTimeTo',
            'createdTimeFrom',
            'createdTimeTo',
            'endTimeFrom',
            'endTimeTo',
        ] as $dateProp) {
            if ($this->$dateProp !== null) {
                $query[$dateProp] = $this->$dateProp->format(DATE_ATOM);
            }
        }

        $query = array_filter(
            $query,
            fn($value) => $value !== null,
        );

        return $query;
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

interface JobInterface extends PlainJobInterface
{
    public function getComponentSpecification(): ComponentSpecification;

    public function getComponentConfiguration(): array;

    public function getExecutionTokenDecrypted(string $applicationToken): string;

    public function getTokenDecrypted(): string;

    public function getComponentConfigurationDecrypted(): ?array;

    public function getConfigDataDecrypted(): array;

    public function getProjectFeatures(): array;
}

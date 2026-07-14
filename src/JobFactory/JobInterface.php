<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

interface JobInterface extends PlainJobInterface
{
    public function getComponentSpecification(): ComponentSpecification;

    public function getComponentConfiguration(): array;

    /**
     * $applicationToken authorizes minting the privileged execution token: pass an explicit
     * application (manage) token, or null to fall back to service identity authentication
     * (the connection ServiceAccount) - the same behaviour as the manage API client.
     *
     * @param non-empty-string|null $applicationToken
     */
    public function getExecutionTokenDecrypted(?string $applicationToken): string;

    public function getTokenDecrypted(): string;

    public function getComponentConfigurationDecrypted(): ?array;

    public function getConfigDataDecrypted(): array;

    public function getProjectFeatures(): array;
}

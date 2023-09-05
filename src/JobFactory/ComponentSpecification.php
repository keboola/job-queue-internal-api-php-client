<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\Exception\ComponentInvalidException;
use Keboola\JobQueueInternalClient\JobFactory\Configuration\ComponentDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ComponentSpecification
{
    private array $definition;

    /**
     * Component constructor.
     * @param array $componentData Component data as returned by Storage API
     */
    public function __construct(array $componentData)
    {
        $componentDefinition = new ComponentDefinition();
        try {
            $this->definition = $componentDefinition->processData($componentData);
        } catch (InvalidConfigurationException $e) {
            throw new ComponentInvalidException(
                'Component definition is invalid. Verify the deployment setup and the repository settings ' .
                'in the Developer Portal. ' . $e->getMessage(),
                0,
                $e,
            );
        }
    }

    public function getId(): string
    {
        return $this->definition['id'];
    }

    public function getMemoryLimit(): string
    {
        return $this->definition['data']['memory'];
    }

    public function getMemoryLimitBytes(): int
    {
        return UnitConverter::connectionMemoryLimitToBytes($this->getMemoryLimit());
    }
}

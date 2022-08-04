<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Closure;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Processor;

class OverridesConfigurationDefinition implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('overrides');
        $this->getRootDefinition($treeBuilder);
        return $treeBuilder;
    }

    public function processData(array $runtimeConfiguration): array
    {
        $processor = new Processor();
        return $processor->processConfiguration($this, [$runtimeConfiguration]);
    }

    protected function getRootDefinition(TreeBuilder $treeBuilder): ArrayNodeDefinition
    {
        /** @var ArrayNodeDefinition $rootNode */
        $rootNode = $treeBuilder->getRootNode();
        // @formatter:off
        $rootNode
            ->ignoreExtraKeys(true)
            ->children()
                ->scalarNode('variableValuesId')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->arrayNode('variableValuesData')->ignoreExtraKeys(true)
                    ->children()
                        ->arrayNode('values')
                            ->prototype('array')
                                ->children()
                                    ->scalarNode('name')->isRequired()->cannotBeEmpty()->end()
                                    ->scalarNode('value')->isRequired()->cannotBeEmpty()->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('runtime')
                ->ignoreExtraKeys(true)
                    ->children()
                        ->scalarNode('tag')
                            ->beforeNormalization()->always($this->getStringNormalizer())->end()
                        ->end()
                        // for backwards compatibility with legacy transformation configurations
                        ->scalarNode('image_tag')
                            ->beforeNormalization()->always($this->getStringNormalizer())->end()
                        ->end()
                        ->arrayNode('backend')->ignoreExtraKeys(true)
                            ->children()
                                ->scalarNode('type')
                                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                                ->end()
                            ->end()
                        ->end()
                        ->scalarNode('parallelism')
                            ->defaultNull()
                            ->validate()
                                ->ifNotInArray(Job::getAllowedParallelismValues())
                                ->thenInvalid(
                                    'Parallelism value must be either null, an integer from range 2-100 or "infinity".'
                                )
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end();
        // @formatter:on
        return $rootNode;
    }

    private function getStringNormalizer(): Closure
    {
        return function ($v) {
            if (is_scalar($v)) {
                return empty($v) ? null : (string) $v;
            } else {
                return $v;
            }
        };
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Processor;

class NewJobDefinition implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('job');
        $this->getRootDefinition($treeBuilder);
        return $treeBuilder;
    }

    public function processData(array $jobData): array
    {
        $processor = new Processor();
        return $processor->processConfiguration($this, [$jobData]);
    }

    protected function getRootDefinition(TreeBuilder $treeBuilder): ArrayNodeDefinition
    {
        /** @var ArrayNodeDefinition $rootNode */
        $rootNode = $treeBuilder->getRootNode();
        // @formatter:off
        $rootNode
            ->children()
                ->scalarNode('#tokenString')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('configId')->end()
                ->scalarNode('componentId')->isRequired()->cannotBeEmpty()->end()
                ->arrayNode('result')->ignoreExtraKeys(false)->end()
                ->scalarNode('mode')->defaultValue('run')
                    ->validate()
                        ->ifNotInArray(['run', 'debug'])
                        ->thenInvalid('Mode must be one of "run" or "debug".')
                    ->end()
                ->end()
                ->arrayNode('configRowIds')
                    ->prototype('scalar')
                    ->end()
                ->end()
                ->scalarNode('tag')->end()
                ->scalarNode('parentRunId')->end()
                ->arrayNode('configData')->ignoreExtraKeys(false)->end()
                ->scalarNode('branchId')->end()
                ->scalarNode('variableValuesId')->end()
                ->arrayNode('variableValuesData')
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
                ->arrayNode('backend')->ignoreExtraKeys(false)
                    ->children()
                        ->scalarNode('type')->end()
                    ->end()
                ->end()
            ->end();
        // @formatter:on
        return $rootNode;
    }
}

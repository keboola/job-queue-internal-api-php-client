<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class FullJobDefinition extends NewJobDefinition
{
    protected function getRootDefinition(TreeBuilder $treeBuilder): ArrayNodeDefinition
    {
        /** @var ArrayNodeDefinition $rootNode */
        $rootNode = $treeBuilder->getRootNode();
        // @formatter:off
        $rootNode
            ->children()
                ->arrayNode('token')->isRequired()
                    ->children()
                        ->scalarNode('id')->isRequired()->cannotBeEmpty()->end()
                        ->scalarNode('token')->isRequired()->cannotBeEmpty()->end()
                    ->end()
                ->end()
                ->arrayNode('project')->isRequired()
                    ->children()
                        ->scalarNode('id')->isRequired()->cannotBeEmpty()->end()
                    ->end()
                ->end()
                ->arrayNode('params')->isRequired()
                    ->children()
                        ->scalarNode('config')->end()
                        ->scalarNode('component')->isRequired()->cannotBeEmpty()->end()
                        ->scalarNode('mode')->isRequired()
                            ->validate()
                                ->ifNotInArray(['run', 'debug'])
                                ->thenInvalid('Mode must be one of "run" or "debug".')
                            ->end()
                        ->end()
                        ->scalarNode('row')->defaultNull()->end()
                        ->scalarNode('tag')->defaultNull()->end()
                        ->arrayNode('configData')->ignoreExtraKeys(false)->end()
                    ->end()
                ->end()
                ->scalarNode('status')->isRequired()
                    ->validate()
                        ->ifNotInArray(JobFactory::getAllStatuses())
                        ->thenInvalid('Status must be one of ' . implode(', ', JobFactory::getAllStatuses()) . '.')
                    ->end()
                ->end()
                ->arrayNode('result')->ignoreExtraKeys(false)->end()
                ->scalarNode('createdTime')->end()
                ->scalarNode('id')->isRequired()->cannotBeEmpty()->end()
            ->end();
        // @formatter:on

        return $rootNode;
    }
}

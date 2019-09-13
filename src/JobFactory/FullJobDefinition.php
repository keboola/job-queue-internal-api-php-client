<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\NodeDefinition;
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
                ->scalarNode('id')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('runId')->end()
                ->scalarNode('lockName')->end()
                ->scalarNode('component')->end()
                ->scalarNode('command')->end()
                ->arrayNode('result')->ignoreExtraKeys(false)->end()
                ->scalarNode('createdTime')->end()
                ->scalarNode('startTime')->end()
                ->scalarNode('endTime')->end()
                ->scalarNode('durationSeconds')->end()
                ->scalarNode('waitSeconds')->end()
                ->scalarNode('nestingLevel')->end()
                ->scalarNode('error')->end()
                ->scalarNode('errorNote')->end()
                ->arrayNode('terminatedBy')->ignoreExtraKeys(false)->end()
                ->arrayNode('usage')->ignoreExtraKeys(false)->end()
                ->scalarNode('status')->isRequired()
                    ->validate()
                        ->ifNotInArray(JobFactory::getAllStatuses())
                        ->thenInvalid('Status must be one of ' . implode(', ', JobFactory::getAllStatuses()) . '.')
                    ->end()
                ->end()
                ->append($this->addTokenNode())
                ->append($this->addProjectNode())
                ->append($this->addParamsNode())
            ->end();
        // @formatter:on

        return $rootNode;
    }

    private function addProjectNode(): NodeDefinition
    {
        $treeBuilder = new TreeBuilder('project');

        /** @var ArrayNodeDefinition $node */
        $node = $treeBuilder->getRootNode();

        $node->isRequired()
            ->children()
                ->scalarNode('id')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('name')->end()
            ->end()
        ->end();

        return $node;
    }

    private function addTokenNode(): NodeDefinition
    {
        $treeBuilder = new TreeBuilder('token');

        /** @var ArrayNodeDefinition $node */
        $node = $treeBuilder->getRootNode();

        $node->isRequired()
            ->children()
                ->scalarNode('id')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('description')->end()
                ->scalarNode('token')->isRequired()->cannotBeEmpty()->end()
            ->end()
        ->end();

        return $node;
    }

    private function addParamsNode(): NodeDefinition
    {
        $treeBuilder = new TreeBuilder('params');

        /** @var ArrayNodeDefinition $node */
        $node = $treeBuilder->getRootNode();

        $node->isRequired()
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
        ->end();

        return $node;
    }
}

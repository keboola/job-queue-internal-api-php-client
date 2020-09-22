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
                ->scalarNode('id')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('runId')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
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
                ->scalarNode('encrypted')->end()
                ->arrayNode('terminatedBy')->ignoreExtraKeys(false)->end()
                ->arrayNode('usage')->ignoreExtraKeys(false)->end()
                ->scalarNode('status')->isRequired()
                    ->validate()
                        ->ifNotInArray(JobFactory::getAllStatuses())
                        ->thenInvalid('Status must be one of ' . implode(', ', JobFactory::getAllStatuses()) . '.')
                    ->end()
                ->end()
                ->arrayNode('process')->ignoreExtraKeys(false)->end()
                ->scalarNode('isFinished')->end()
                ->scalarNode('url')->end()
                ->scalarNode('_index')->end()
                ->scalarNode('_type')->end()
                ->append($this->addTokenNode())
                ->append($this->addProjectNode())
                ->append($this->addParamsNode())
            ->end();
        // @formatter:on

        return $rootNode;
    }

    private function getStringNormalizer(): \Closure
    {
        return function ($v) {
            if (is_scalar($v)) {
                return empty($v) ? null : (string)$v;
            } else {
                return $v;
            }
        };
    }

    private function addProjectNode(): NodeDefinition
    {
        $treeBuilder = new TreeBuilder('project');

        /** @var ArrayNodeDefinition $node */
        $node = $treeBuilder->getRootNode();

        $node->isRequired()
            ->children()
                ->scalarNode('id')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('name')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
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
                ->scalarNode('id')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('description')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('token')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
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
            ->ignoreExtraKeys(false)
            ->children()
                ->scalarNode('config')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('component')
                    ->cannotBeEmpty()->isRequired()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('mode')
                    ->validate()
                        ->ifNotInArray(['run', 'debug',
                            // these are only for compatibility with transformation jobs, not used on new jobs
                            'dry-run', 'prepare', 'input', 'full', 'single',
                        ])
                        ->thenInvalid(
                            'Mode must be one of "run" or "debug" (or "dry-run","prepare","input","full","single").'
                        )
                    ->end()
                ->end()
                ->scalarNode('row')
                    ->defaultNull()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('tag')
                    ->defaultNull()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->arrayNode('configData')->ignoreExtraKeys(false)->end()
            ->end()
        ->end();

        return $node;
    }
}

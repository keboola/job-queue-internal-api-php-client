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
                ->scalarNode('projectId')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('projectName')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('tokenId')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('tokenDescription')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('tokenString')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('componentId')
                    ->cannotBeEmpty()->isRequired()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('configId')
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
                ->scalarNode('configRowId')
                    ->defaultNull()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('tag')
                    ->defaultNull()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->arrayNode('configData')->ignoreExtraKeys(false)->end()
                ->scalarNode('createdTime')->end()
                ->scalarNode('startTime')->end()
                ->scalarNode('endTime')->end()
                ->scalarNode('durationSeconds')->end()
                ->arrayNode('result')->ignoreExtraKeys(false)->end()
                ->arrayNode('usageData')->ignoreExtraKeys(false)->end()
                ->scalarNode('status')->isRequired()
                    ->validate()
                        ->ifNotInArray(JobFactory::getAllStatuses())
                        ->thenInvalid('Status must be one of ' . implode(', ', JobFactory::getAllStatuses()) . '.')
                    ->end()
                ->end()
                ->scalarNode('desiredStatus')->isRequired()
                    ->validate()
                        ->ifNotInArray(JobFactory::getAllStatuses())
                        ->thenInvalid('Status must be one of ' . implode(', ', JobFactory::getAllStatuses()) . '.')
                    ->end()
                ->end()
                ->scalarNode('isFinished')
                    ->defaultFalse()
                ->end()
                ->scalarNode('url')->end()
            ->end();
        // @formatter:on

        return $rootNode;
    }

    private function getStringNormalizer(): \Closure
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

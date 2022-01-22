<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Closure;
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
            /* Whatever strange properties make it to the internal API, let's ignore them here and remove, because
                we don't understand them. */
            ->ignoreExtraKeys(true)
            ->children()
                ->scalarNode('id')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('runId')
                    ->isRequired()->cannotBeEmpty()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('parentRunId')
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
                ->scalarNode('#tokenString')
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
                        ->ifNotInArray([Job::MODE_RUN, Job::MODE_FORCE_RUN, Job::MODE_DEBUG,
                            // these are only for compatibility with transformation jobs, not used on new jobs
                            'dry-run', 'prepare', 'input', 'full', 'single',
                        ])
                        ->thenInvalid(
                            'Mode must be one of "run", "forceRun" or "debug" ' .
                            '(or "dry-run","prepare","input","full","single").'
                        )
                    ->end()
                ->end()
                ->arrayNode('configRowIds')
                    ->prototype('scalar')
                        ->beforeNormalization()->always($this->getStringNormalizer())->end()
                    ->end()
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
                        ->ifNotInArray(JobFactory::getAllDesiredStatuses())
                        ->thenInvalid(
                            'DesiredStatus must be one of ' .
                            implode(', ', JobFactory::getAllDesiredStatuses()) .
                            '.'
                        )
                    ->end()
                ->end()
                ->scalarNode('type')
                    ->defaultValue('standard')
                    ->validate()
                        ->ifNotInArray(JobFactory::getAllowedJobTypes())
                        ->thenInvalid('Type must be one of ' . implode(', ', JobFactory::getAllowedJobTypes()) . '.')
                    ->end()
                ->end()
                ->scalarNode('parallelism')
                    ->defaultNull()
                    ->validate()
                        ->ifNotInArray(JobFactory::getAllowedParallelismValues())
                        ->thenInvalid(
                            'Parallelism value must be either null, an integer from range 2-100 or "infinity".'
                        )
                    ->end()
                ->end()
                ->arrayNode('behavior')
                    ->ignoreExtraKeys(true)
                    ->children()
                        ->scalarNode('onError')
                            ->defaultNull()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode('isFinished')
                    ->defaultFalse()
                ->end()
                ->scalarNode('url')->end()
                ->scalarNode('branchId')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('variableValuesId')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
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
                ->arrayNode('backend')->ignoreExtraKeys(true)
                    ->children()
                        ->scalarNode('type')->end()
                    ->end()
                ->end()
                ->arrayNode('metrics')->ignoreExtraKeys(true)
                    ->children()
                        ->arrayNode('storage')
                            ->children()
                                ->scalarNode('inputTablesBytesSum')->end()
                            ->end()
                        ->end()
                    ->end()
                    ->children()
                        ->arrayNode('backend')
                            ->children()
                                ->scalarNode('size')->end()
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

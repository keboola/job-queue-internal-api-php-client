<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Closure;
use Keboola\JobQueueInternalClient\JobFactory\Runtime\Executor;
use Keboola\PermissionChecker\BranchType;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class ElasticJobDefinition extends NewJobDefinition
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
                ->scalarNode('deduplicationId')
                    ->validate()
                        ->ifTrue(fn($v) => $v !== null && !is_string($v))
                        ->thenInvalid('value must be a string')
                    ->end()
                    ->validate()
                        ->ifTrue(fn($v) => $v === '')
                        ->thenInvalid('value cannot be empty string')
                    ->end()
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
                ->scalarNode('componentId')
                    ->cannotBeEmpty()->isRequired()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('configId')
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('mode')
                    ->validate()
                        ->ifNotInArray([
                            JobInterface::MODE_RUN,
                            JobInterface::MODE_FORCE_RUN,
                            JobInterface::MODE_DEBUG,
                            // these are only for compatibility with transformation jobs, not used on new jobs
                            'dry-run', 'prepare', 'input', 'full', 'single',
                        ])
                        ->thenInvalid(
                            'Mode must be one of "run", "forceRun" or "debug" ' .
                            '(or "dry-run","prepare","input","full","single").',
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
                ->scalarNode('delayedStartTime')->end()
                ->scalarNode('durationSeconds')->end()
                ->arrayNode('result')->ignoreExtraKeys(false)->end()
                ->arrayNode('usageData')->ignoreExtraKeys(false)->end()
                ->scalarNode('status')->isRequired()
                    ->validate()
                        ->ifNotInArray(JobInterface::STATUSES_ALL)
                        ->thenInvalid('Status must be one of ' . implode(', ', JobInterface::STATUSES_ALL) . '.')
                    ->end()
                ->end()
                ->scalarNode('desiredStatus')->isRequired()
                    ->validate()
                        ->ifNotInArray(JobInterface::DESIRED_STATUSES_ALL)
                        ->thenInvalid(
                            'DesiredStatus must be one of ' .
                            implode(', ', JobInterface::DESIRED_STATUSES_ALL) .
                            '.',
                        )
                    ->end()
                ->end()
                ->enumNode('type')
                    ->defaultValue(JobType::STANDARD->value)
                    ->values(array_map(fn(JobType $t) => $t->value, JobType::cases()))
                ->end()
                ->scalarNode('parallelism')
                    ->defaultNull()
                    ->validate()
                        ->ifNotInArray(JobInterface::PARALLELISM_ALL)
                        ->thenInvalid(
                            'Parallelism value must be either null, an integer from range 2-100 or "infinity".',
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
                ->enumNode('branchType')
                    ->values(array_map(fn(BranchType $t) => $t->value, BranchType::cases()))
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
                        ->scalarNode('containerType')->end()
                        ->scalarNode('context')->end()
                    ->end()
                ->end()
                ->enumNode('executor')
                    ->values([null, ...array_map(fn(Executor $e) => $e->value, Executor::cases())])
                ->end()
                ->arrayNode('metrics')->ignoreExtraKeys(true)
                    ->children()
                        ->arrayNode('storage')
                            ->children()
                                ->scalarNode('inputTablesBytesSum')->end()
                            ->end()
                            ->children()
                                ->scalarNode('outputTablesBytesSum')->end()
                            ->end()
                        ->end()
                    ->end()
                    ->children()
                        ->arrayNode('backend')->ignoreExtraKeys(true)
                            ->children()
                                ->scalarNode('size')->end()
                                ->scalarNode('containerSize')->end()
                                ->scalarNode('context')->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->scalarNode('orchestrationJobId')
                    ->defaultNull()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
                ->end()
                ->scalarNode('orchestrationTaskId')
                    ->validate()
                        ->ifTrue(fn($v) => $v !== null && !is_string($v))
                        ->thenInvalid('value must be a string')
                    ->end()
                    ->validate()
                        ->ifTrue(fn($v) => $v === '')
                        ->thenInvalid('value cannot be empty string')
                    ->end()
                ->end()
                ->scalarNode('orchestrationPhaseId')
                    ->validate()
                        ->ifTrue(fn($v) => $v !== null && !is_string($v))
                        ->thenInvalid('value must be a string')
                    ->end()
                    ->validate()
                        ->ifTrue(fn($v) => $v === '')
                        ->thenInvalid('value cannot be empty string')
                    ->end()
                ->end()
                ->variableNode('onlyOrchestrationTaskIds')
                    ->validate()
                        ->ifTrue(fn($v) => $v !== null && !is_array($v))
                        ->thenInvalid('value must be an array')
                    ->end()
                    ->validate()
                        ->ifTrue(fn($v) => $v === [])
                        ->thenInvalid('value cannot be empty list')
                    ->end()
                    ->validate()
                        ->ifTrue(fn($v) => count(array_filter($v ?? [], fn($i) => !is_scalar($i))) > 0)
                        ->thenInvalid('items must be scalars')
                    ->end()
                    ->validate()
                        ->ifTrue(fn($v) => count(array_filter($v ?? [], fn($i) => $i === '')) > 0)
                        ->thenInvalid('item cannot be empty string')
                    ->end()
                    ->validate()
                        ->ifTrue(fn($v) => $v !== null && count($v) !== count(array_unique($v)))
                        ->thenInvalid('items must be unique')
                    ->end()
                ->end()
                ->scalarNode('previousJobId')
                    ->validate()
                        ->ifTrue(fn($v) => $v !== null && !is_string($v))
                        ->thenInvalid('value must be a string')
                    ->end()
                    ->validate()
                        ->ifTrue(fn($v) => $v === '')
                        ->thenInvalid('value cannot be empty string')
                    ->end()
                ->end()
                ->scalarNode('runnerId')
                    ->defaultNull()
                    ->beforeNormalization()->always($this->getStringNormalizer())->end()
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

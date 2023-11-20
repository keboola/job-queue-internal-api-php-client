<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use Keboola\JobQueueInternalClient\JobFactory\Runtime\Executor;
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
                ->scalarNode('#tokenString')->isRequired()->cannotBeEmpty()->end()
                ->scalarNode('configId')->end()
                ->scalarNode('componentId')->isRequired()->cannotBeEmpty()->end()
                ->arrayNode('result')->ignoreExtraKeys(false)->end()
                ->scalarNode('mode')->defaultValue('run')
                    ->validate()
                        ->ifNotInArray([
                            JobInterface::MODE_RUN,
                            JobInterface::MODE_DEBUG,
                            JobInterface::MODE_FORCE_RUN,
                        ])
                        ->thenInvalid('Mode must be one of "run", "forceRun" or "debug".')
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
                ->scalarNode('type')->end()
                ->scalarNode('parallelism')->end()
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
                ->arrayNode('behavior')->ignoreExtraKeys(true)
                    ->children()
                        ->scalarNode('onError')->end()
                    ->end()
                ->end()
                ->scalarNode('orchestrationJobId')->end()
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
            ->end();
        // @formatter:on
        return $rootNode;
    }
}

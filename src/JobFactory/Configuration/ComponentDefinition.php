<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory\Configuration;

use Monolog\Logger;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;
use Symfony\Component\Config\Definition\Processor;

class ComponentDefinition implements ConfigurationInterface
{
    public const KNOWN_IMAGE_TYPES = ['dockerhub', 'builder', 'quayio', 'aws-ecr'];

    /**
     * Verbosity None - event will not be stored in Storage at all.
     */
    public const VERBOSITY_NONE = 'none';

    /**
     * Verbosity Camouflage - event will be stored in Storage only as a generic message.
     */
    public const VERBOSITY_CAMOUFLAGE = 'camouflage';

    /**
     * Verbosity Normal - event will be stored in Storage as received.
     */
    public const VERBOSITY_NORMAL = 'normal';

    /**
     * Verbosity Verbose - event will be stored in Storage including all additional event data.
     */
    public const VERBOSITY_VERBOSE = 'verbose';

    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder('component');
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
        $rootNode->ignoreExtraKeys()->children()
            ->scalarNode('id')->isRequired()->cannotBeEmpty()->end()
            ->arrayNode('features')
                ->scalarPrototype()->end()
                ->defaultValue([])
            ->end()
            ->arrayNode('data')->isRequired()
                ->children()
                    ->arrayNode('definition')->isRequired()
                        ->children()
                            ->scalarNode('type')
                                ->isRequired()
                                ->validate()
                                    ->ifNotInArray(self::KNOWN_IMAGE_TYPES)
                                    ->thenInvalid('Invalid image type %s.')
                                ->end()
                            ->end()
                            ->scalarNode('uri')->isRequired()->cannotBeEmpty()->end()
                            ->scalarNode('tag')->defaultValue('latest')->end()
                            ->scalarNode('digest')->defaultValue('')->end()
                            ->arrayNode('repository')
                                ->children()
                                    ->scalarNode('region')->end()
                                    ->scalarNode('username')->end()
                                    ->scalarNode('#password')->end()
                                    ->scalarNode('server')->end()
                                ->end()
                            ->end()
                            ->arrayNode('build_options')
                                ->children()
                                    ->scalarNode('parent_type')
                                        ->isRequired()
                                        ->validate()
                                            ->ifNotInArray(self::KNOWN_IMAGE_TYPES)
                                            ->thenInvalid('Invalid image type %s.')
                                        ->end()
                                    ->end()
                                    ->arrayNode('repository')
                                        ->isRequired()
                                        ->children()
                                            ->scalarNode('uri')->isRequired()->end()
                                            ->scalarNode('type')
                                                ->isRequired()
                                                ->validate()
                                                    ->ifNotInArray(['git'])
                                                    ->thenInvalid('Invalid repository_type %s.')
                                                ->end()
                                            ->end()
                                            ->scalarNode('username')->end()
                                            ->scalarNode('#password')->end()
                                        ->end()
                                    ->end()
                                    ->scalarNode('entry_point')->isRequired()->end()
                                    ->arrayNode('commands')
                                        ->prototype('scalar')->end()
                                    ->end()
                                    ->arrayNode('parameters')
                                        ->prototype('array')
                                            ->children()
                                                ->scalarNode('name')->isRequired()->end()
                                                ->booleanNode('required')->defaultValue(true)->end()
                                                ->scalarNode('type')
                                                    ->isRequired()
                                                    ->validate()
                                                        ->ifNotInArray(['int', 'string', 'argument',
                                                            'plain_string', 'enumeration',
                                                        ])
                                                        ->thenInvalid('Invalid image type %s.')
                                                    ->end()
                                                ->end()
                                                ->scalarNode('default_value')->defaultValue(null)->end()
                                                ->arrayNode('values')->prototype('scalar')->end()->end()
                                            ->end()
                                        ->end()
                                    ->end()
                                    ->scalarNode('version')->end()
                                    ->booleanNode('cache')->defaultValue(true)->end()
                                ->end()
                            ->end()
                        ->end()
                    ->end()
                    ->scalarNode('memory')->defaultValue('256m')->end()
                    ->scalarNode('configuration_format')
                        ->defaultValue('json')
                        ->validate()
                            ->ifNotInArray(['yaml', 'json'])
                            ->thenInvalid('Invalid configuration_format %s.')
                        ->end()
                    ->end()
                    ->integerNode('process_timeout')->min(0)->defaultValue(3600)->end()
                    ->booleanNode('forward_token')->defaultValue(false)->end()
                    ->booleanNode('forward_token_details')->defaultValue(false)->end()
                    ->booleanNode('default_bucket')->defaultValue(false)->end()
                    ->variableNode('image_parameters')->defaultValue([])->end()
                    ->scalarNode('network')
                        ->validate()
                            ->ifNotInArray(['none', 'bridge', 'no-internet'])
                            ->thenInvalid('Invalid network type %s.')
                        ->end()
                        ->defaultValue('bridge')
                    ->end()
                    ->scalarNode('default_bucket_stage')
                        ->validate()
                            ->ifNotInArray(['in', 'out'])
                            ->thenInvalid('Invalid default_bucket_stage %s.')
                        ->end()
                        ->defaultValue('in')
                    ->end()
                    ->variableNode('vendor')->end()
                    ->arrayNode('synchronous_actions')->prototype('scalar')->end()->end()
                    ->arrayNode('logging')
                        ->addDefaultsIfNotSet()
                        ->children()
                            ->scalarNode('type')
                               ->validate()
                                    ->ifNotInArray(['standard', 'gelf'])
                                    ->thenInvalid('Invalid logging type %s.')
                                ->end()
                                ->defaultValue('standard')
                            ->end()
                            ->arrayNode('verbosity')
                                ->prototype('scalar')->end()
                                ->defaultValue([
                                    Logger::DEBUG => self::VERBOSITY_NONE,
                                    Logger::INFO => self::VERBOSITY_NORMAL,
                                    Logger::NOTICE => self::VERBOSITY_NORMAL,
                                    Logger::WARNING => self::VERBOSITY_NORMAL,
                                    Logger::ERROR => self::VERBOSITY_NORMAL,
                                    Logger::CRITICAL => self::VERBOSITY_CAMOUFLAGE,
                                    Logger::ALERT => self::VERBOSITY_CAMOUFLAGE,
                                    Logger::EMERGENCY => self::VERBOSITY_CAMOUFLAGE,
                                ])
                            ->end()
                            ->scalarNode('gelf_server_type')
                                ->validate()
                                    ->ifNotInArray(['tcp', 'udp', 'http'])
                                    ->thenInvalid('Invalid GELF server type %s.')
                                ->end()
                                ->defaultValue('tcp')
                            ->end()
                            ->booleanNode('no_application_errors')
                                ->defaultValue(false)
                            ->end()
                        ->end()
                    ->end()
                    ->arrayNode('staging_storage')
                        ->addDefaultsIfNotSet()
                        ->children()
                            ->enumNode('input')
                                ->values(['local', 's3', 'abs', 'none', 'workspace-snowflake', 'workspace-redshift',
                                    'workspace-synapse', 'workspace-abs', 'workspace-exasol', 'workspace-teradata',
                                    'workspace-bigquery',
                                ])
                                ->defaultValue('local')
                            ->end()
                            ->enumNode('output')
                                ->values(['local', 'none', 'workspace-snowflake', 'workspace-redshift',
                                    'workspace-synapse', 'workspace-abs', 'workspace-exasol', 'workspace-teradata',
                                    'workspace-bigquery',
                                ])
                                ->defaultValue('local')
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ->end();
        // @formatter:on
        return $rootNode;
    }
}

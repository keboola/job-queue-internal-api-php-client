<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobIdGenerator;

use Keboola\JobQueueInternalClient\Client;
use Keboola\JobQueueInternalClient\JobIdGenerator\InternalApiJobIdGenerator;
use Keboola\JobQueueInternalClient\JobIdGenerator\JobIdGeneratorFactory;
use Keboola\JobQueueInternalClient\JobIdGenerator\StorageTicketJobIdGenerator;
use PHPUnit\Framework\TestCase;
use ValueError;

class JobIdGeneratorFactoryTest extends TestCase
{
    public static function provideType(): iterable
    {
        yield 'internal API' => [
            'type' => 'internal',
            'expectedClass' => InternalApiJobIdGenerator::class,
        ];
        yield 'Storage ticket' => [
            'type' => 'storage',
            'expectedClass' => StorageTicketJobIdGenerator::class,
        ];
    }

    /**
     * @dataProvider provideType
     * @param class-string $expectedClass
     */
    public function testCreate(string $type, string $expectedClass): void
    {
        $factory = new JobIdGeneratorFactory($this->createMock(Client::class));

        self::assertInstanceOf($expectedClass, $factory->create($type));
    }

    public function testCreateWithUnknownType(): void
    {
        $factory = new JobIdGeneratorFactory($this->createMock(Client::class));

        $this->expectException(ValueError::class);
        $this->expectExceptionMessage('Unknown job ID generator "foo". Use "internal" or "storage".');

        $factory->create('foo');
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\JobFactory\ObjectEncryptorProvider;

use Keboola\JobQueueInternalClient\JobFactory\ObjectEncryptorProvider\GenericObjectEncryptorProvider;
use Keboola\ObjectEncryptor\ObjectEncryptor;
use PHPUnit\Framework\TestCase;

class GenericObjectEncryptorProviderTest extends TestCase
{
    public function testGetEncryptor(): void
    {
        $objectEncryptor = $this->createMock(ObjectEncryptor::class);
        $objectEncryptor->expects(self::once())
            ->method('decryptForProject')
            ->with('encryptedData', 'componentId', 'projectId')
            ->willReturn('data')
        ;

        $provider = new GenericObjectEncryptorProvider($objectEncryptor);
        $result = $provider->getJobEncryptor(['id' => 'jobId']);

        // this should call decryptForProject and match the objectEncryptor mock expectation
        $result->decrypt('encryptedData', 'componentId', 'projectId', null);
    }
}

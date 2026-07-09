<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Authenticator;

use GuzzleHttp\Psr7\Request;
use InvalidArgumentException;
use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Keboola\JobQueueInternalClient\Authenticator\InternalApiTokenAuthenticator;
use PHPUnit\Framework\TestCase;

class InternalApiTokenAuthenticatorTest extends TestCase
{
    public function testAddsHeader(): void
    {
        $authenticator = new InternalApiTokenAuthenticator('my-token');
        self::assertInstanceOf(RequestAuthenticatorInterface::class, $authenticator);

        $request = $authenticator(new Request('GET', 'https://example.com/'));

        self::assertSame('my-token', $request->getHeaderLine('X-JobQueue-InternalApi-Token'));
    }

    public function testEmptyTokenThrows(): void
    {
        $this->expectException(InvalidArgumentException::class);
        new InternalApiTokenAuthenticator('');
    }
}

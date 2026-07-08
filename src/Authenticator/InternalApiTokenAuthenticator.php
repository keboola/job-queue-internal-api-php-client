<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Authenticator;

use Keboola\ApiClientBase\Auth\RequestAuthenticatorInterface;
use Psr\Http\Message\RequestInterface;
use SensitiveParameter;
use Webmozart\Assert\Assert;

final readonly class InternalApiTokenAuthenticator implements RequestAuthenticatorInterface
{
    public const HEADER = 'X-JobQueue-InternalApi-Token';

    public function __construct(
        #[SensitiveParameter]
        private string $token,
    ) {
        Assert::stringNotEmpty($token, 'Internal API token must not be empty');
    }

    public function __invoke(RequestInterface $request): RequestInterface
    {
        return $request->withHeader(self::HEADER, $this->token);
    }
}

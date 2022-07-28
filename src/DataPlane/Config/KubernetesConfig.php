<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane\Config;

use Keboola\ObjectEncryptor\ObjectEncryptor;

class KubernetesConfig
{
    private string $apiUrl;
    private string $token;
    private string $certificateAuthority;
    private string $namespace;

    public function __construct(string $apiUrl, string $token, string $certificateAuthority, string $namespace)
    {
        $this->apiUrl = $apiUrl;
        $this->token = $token;
        $this->certificateAuthority = $certificateAuthority;
        $this->namespace = $namespace;
    }

    public function getApiUrl(): string
    {
        return $this->apiUrl;
    }

    public function getToken(): string
    {
        return $this->token;
    }

    public function getTokenDecrypted(ObjectEncryptor $encryptor): string
    {
        return $encryptor->decryptGeneric($this->token);
    }

    public function getCertificateAuthority(): string
    {
        return $this->certificateAuthority;
    }

    public function getNamespace(): string
    {
        return $this->namespace;
    }
}

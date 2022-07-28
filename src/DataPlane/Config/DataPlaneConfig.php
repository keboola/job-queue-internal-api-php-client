<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane\Config;

use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\EncryptionConfigInterface;

class DataPlaneConfig
{
    private string $id;
    private KubernetesConfig $kubernetes;
    private EncryptionConfigInterface $encryption;

    public function __construct(
        string $id,
        KubernetesConfig $kubernetes,
        EncryptionConfigInterface $encryption
    ) {
        $this->id = $id;
        $this->kubernetes = $kubernetes;
        $this->encryption = $encryption;
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getKubernetes(): KubernetesConfig
    {
        return $this->kubernetes;
    }

    public function getEncryption(): EncryptionConfigInterface
    {
        return $this->encryption;
    }
}

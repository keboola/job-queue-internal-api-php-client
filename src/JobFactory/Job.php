<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\JobFactory;

use JsonSerializable;
use Keboola\JobQueueInternalClient\JobFactory;
use Keboola\ObjectEncryptor\ObjectEncryptorFactory;

class Job implements JsonSerializable
{
    public const RUN_ID_DELIMITER = '.';

    /** @var array */
    private $data;

    /** @var ObjectEncryptorFactory */
    private $objectEncryptorFactory;

    public function __construct(ObjectEncryptorFactory $objectEncryptorFactory, array $data)
    {
        $this->data = $data;
        // while it would make more sense to store parentRunId, i want to keep this compatible with the old queue ATM
        if (!empty($this->data['parentRunId'])) {
            $this->data['runId'] = $this->data['parentRunId'] . self::RUN_ID_DELIMITER . $this->data['id'];
            unset($this->data['parentRunId']);
        } else {
            $this->data['runId'] = $this->data['id'];
        }
        // it's important to clone here because we change state of the factory!
        // this is tested by JobFactoryTest::testEncryptionMultipleJobs()
        $this->objectEncryptorFactory = clone $objectEncryptorFactory;
        $this->objectEncryptorFactory->setProjectId($this->getProjectId());
        $this->objectEncryptorFactory->setComponentId($this->getComponentId());
        $this->objectEncryptorFactory->setConfigurationId($this->getConfigId());
    }

    public function getComponentId(): string
    {
        return $this->data['params']['component'] ?? '';
    }

    public function getConfigData(): array
    {
        return $this->data['params']['configData'] ?? [];
    }

    public function getConfigId(): ?string
    {
        if (empty($this->data['params']['config'])) {
            return null;
        } else {
            return (string) $this->data['params']['config'];
        }
    }

    public function getId(): string
    {
        return (string) $this->data['id'];
    }

    public function getMode(): string
    {
        return $this->data['params']['mode'];
    }

    public function getProjectId(): string
    {
        return (string) $this->data['project']['id'];
    }

    public function getResult(): array
    {
        return $this->data['result'] ?? [];
    }

    public function getRowId(): ?string
    {
        if (empty($this->data['params']['row'])) {
            return null;
        } else {
            return (string) $this->data['params']['row'];
        }
    }

    public function getStatus(): string
    {
        return $this->data['status'];
    }

    public function getTag(): ?string
    {
        if (empty($this->data['params']['tag'])) {
            return null;
        } else {
            return (string) $this->data['params']['tag'];
        }
    }

    public function getToken(): string
    {
        return $this->data['token']['token'];
    }

    public function getParentRunId(): string
    {
        return (string) $this->data['parentRunId'] ?? '';
    }

    public function getRunId(): string
    {
        $parentRunId = $this->getParentRunId();
        if ($parentRunId) {
            return $parentRunId . self::RUN_ID_DELIMITER . $this->getId();
        } else {
            return $this->getId();
        }
    }

    public function isFinished(): bool
    {
        return in_array($this->data['status'], JobFactory::getFinishedStatuses());
    }

    public function jsonSerialize(): array
    {
        return $this->data;
    }

    public function getTokenDecrypted(): string
    {
        return $this->objectEncryptorFactory->getEncryptor(true)->decrypt($this->getToken());
    }

    public function getConfigDataDecrypted(): array
    {
        return $this->objectEncryptorFactory->getEncryptor()->decrypt($this->getConfigData());
    }

    public function isLegacyComponent(): bool
    {
        return empty($this->getComponentId()) || in_array($this->getComponentId(), JobFactory::getLegacyComponents());
    }
}

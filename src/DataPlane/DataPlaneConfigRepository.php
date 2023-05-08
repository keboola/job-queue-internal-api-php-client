<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane;

use Keboola\JobQueueInternalClient\DataPlane\Config\DataPlaneConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\Encryption\AwsEncryptionConfig;
use Keboola\JobQueueInternalClient\DataPlane\Config\KubernetesConfig;
use Keboola\JobQueueInternalClient\DataPlane\Exception\DataPlaneNotFoundException;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException;
use RuntimeException;

class DataPlaneConfigRepository
{
    private ManageApiClient $manageApiClient;
    private DataPlaneConfigValidator $configValidator;

    /** @var non-empty-string */
    private string $stackId;

    /** @var non-empty-string|null  */
    private ?string $kmsRegion;

    /**
     * @param non-empty-string      $stackId
     * @param non-empty-string|null $kmsRegion
     */
    public function __construct(
        ManageApiClient $manageApiClient,
        DataPlaneConfigValidator $configValidator,
        string $stackId,
        ?string $kmsRegion
    ) {
        $this->manageApiClient = $manageApiClient;
        $this->configValidator = $configValidator;
        $this->stackId = $stackId;
        $this->kmsRegion = $kmsRegion;
    }

    public function fetchProjectDataPlane(string $projectId): ?DataPlaneConfig
    {
        $project = $this->manageApiClient->getProject($projectId);

        // always use the first configured data plane
        $dataPlane = $project['dataPlanes'][0] ?? null;

        if ($dataPlane === null) {
            return null;
        }

        return $this->mapDataPlaneConfig(
            (string) $dataPlane['id'],
            $dataPlane['parameters'] ?? [],
        );
    }

    public function fetchDataPlaneConfig(string $dataPlaneId): DataPlaneConfig
    {
        if (!ctype_digit($dataPlaneId)) {
            throw new RuntimeException(sprintf(
                'Invalid data plane ID "%s". The value must be an integer.',
                $dataPlaneId
            ));
        }

        try {
            $dataPlane = $this->manageApiClient->getDataPlane((int) $dataPlaneId);
        } catch (ClientException $e) {
            if ($e->getCode() === 404) {
                throw new DataPlaneNotFoundException(sprintf(
                    'Data plane "%s" not found',
                    $dataPlaneId
                ), 0, $e);
            }

            throw $e;
        }

        return $this->mapDataPlaneConfig(
            $dataPlaneId,
            $dataPlane['parameters'] ?? [],
        );
    }

    /**
     * @return DataPlaneConfig[]
     */
    public function listDataPlaneConfigs(): array
    {
        return array_map(
            fn(array $data) => $this->mapDataPlaneConfig((string) $data['id'], $data['parameters']),
            $this->manageApiClient->listDataPlanes()
        );
    }

    private function mapDataPlaneConfig(string $dataPlaneId, array $data): DataPlaneConfig
    {
        $data = $this->configValidator->validateDataPlaneConfig($dataPlaneId, $data);

        $kubernetesData = $data['kubernetes'];
        $kubernetesConfig = new KubernetesConfig(
            $kubernetesData['apiUrl'],
            $kubernetesData['#token'],
            $kubernetesData['certificateAuthority'],
            $kubernetesData['namespace'],
        );

        $encryptionData = $data['encryption'];
        switch ($encryptionData['type']) {
            case DataPlaneConfigValidator::ENCRYPTION_TYPE_AWS:
                if ($this->kmsRegion === null) {
                    throw new RuntimeException('Can\'t create AWS encryption config, the KMS region is not set.');
                }

                $encryptionConfig = new AwsEncryptionConfig(
                    $this->stackId,
                    $this->kmsRegion,
                    $encryptionData['kmsKeyId'],
                    $encryptionData['kmsRoleArn'],
                );
                break;

            default:
                throw new RuntimeException(sprintf(
                    'Invalid encryption type "%s"',
                    $encryptionData['type']
                ));
        }

        return new DataPlaneConfig(
            $dataPlaneId,
            $kubernetesConfig,
            $encryptionConfig,
        );
    }
}

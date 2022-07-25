<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\DataPlane;

use Keboola\JobQueueInternalClient\DataPlane\Exception\DataPlaneNotFoundException;
use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ManageApi\ClientException;
use RuntimeException;

class DataPlaneConfigRepository
{
    private ManageApiClient $manageApiClient;
    private DataPlaneConfigValidator $configValidator;

    public function __construct(ManageApiClient $manageApiClient, DataPlaneConfigValidator $configValidator)
    {
        $this->manageApiClient = $manageApiClient;
        $this->configValidator = $configValidator;
    }

    /**
     * @return null|array{
     *     id: string,
     *     parameters: array{
     *         kubernetes: array{
     *             apiUrl: string,
     *             token: string,
     *             certificateAuthority: string,
     *             namespace: string,
     *         },
     *         encryption: array{
     *             type: 'aws',
     *             kmsKeyId: string,
     *             encryptionRoleArn: string,
     *         },
     *     }
     * }
     */
    public function fetchProjectDataPlane(string $projectId): ?array
    {
        $project = $this->manageApiClient->getProject($projectId);

        // always use the first configured data plane
        $dataPlane = $project['dataPlanes'][0] ?? null;

        if ($dataPlane === null) {
            return null;
        }

        $dataPlaneId = (string) $dataPlane['id'];
        $dataPlaneConfig = $this->configValidator->validateDataPlaneConfig(
            $dataPlaneId,
            $dataPlane['parameters'] ?? []
        );

        return [
            'id' => $dataPlaneId,
            'parameters' => $dataPlaneConfig,
        ];
    }

    /**
     * @return array{
     *     kubernetes: array{
     *         apiUrl: string,
     *         token: string,
     *         certificateAuthority: string,
     *         namespace: string,
     *     },
     *     encryption: array{
     *         type: 'aws',
     *         kmsKeyId: string,
     *         encryptionRoleArn: string,
     *     },
     * }
     */
    public function fetchDataPlaneConfig(string $dataPlaneId): array
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

        return $this->configValidator->validateDataPlaneConfig($dataPlaneId, $dataPlane['parameters'] ?? []);
    }
}

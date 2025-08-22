<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;
use Keboola\Artifacts\Result;

class JobArtifacts implements JsonSerializable
{
    /** @var Result[] */
    private array $uploaded = [];

    /** @var Result[] */
    private array $downloaded = [];

    /**
     * @param Result[] $uploadedFiles
     * @return $this
     */
    public function setUploaded(array $uploadedFiles): self
    {
        $this->uploaded = $uploadedFiles;
        return $this;
    }

    /**
     * @param Result[] $downloadedFiles
     * @return $this
     */
    public function setDownloaded(array $downloadedFiles): self
    {
        $this->downloaded = $downloadedFiles;
        return $this;
    }

    /** @param Result[] $downloadedArtifacts */
    public function addDownloaded(array $downloadedArtifacts): void
    {
        $this->downloaded = array_merge($this->downloaded, $downloadedArtifacts);
    }

    /** @param Result[] $uploadedArtifacts */
    public function addUploaded(array $uploadedArtifacts): void
    {
        $this->uploaded = array_merge($this->uploaded, $uploadedArtifacts);
    }

    public function jsonSerialize(): array
    {
        return [
            'uploaded' => array_map(
                $this->getSerializeResultCallable(),
                $this->uploaded,
            ),
            'downloaded' => array_map(
                $this->getSerializeResultCallable(),
                $this->downloaded,
            ),
        ];
    }

    private function getSerializeResultCallable(): callable
    {
        return fn (Result $result) => [
            'storageFileId' => (string) $result->getStorageFileId(),
        ];
    }
}

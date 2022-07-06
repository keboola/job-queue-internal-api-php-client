<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;

class Artifacts implements JsonSerializable
{
    private ?array $uploaded;

    private ?array $downloaded;

    public function setUploaded(array $uploadedFile): Artifacts
    {
        $this->uploaded = $uploadedFile;
        return $this;
    }

    public function setDownloaded(array $downloadedFiles): Artifacts
    {
        $this->downloaded = $downloadedFiles;
        return $this;
    }

    public function jsonSerialize(): array
    {
        return [
            'uploaded' => $this->uploaded,
            'downloaded' => $this->downloaded,
        ];
    }
}

<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Result;

use JsonSerializable;

class Artifacts implements JsonSerializable
{
    private ?array $uploaded = null;

    private ?array $downloaded = null;

    public function setUploaded(array $uploadedFiles): Artifacts
    {
        $this->uploaded = $uploadedFiles;
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

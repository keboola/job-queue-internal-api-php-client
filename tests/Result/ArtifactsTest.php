<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result;

use Keboola\Artifacts\Result;
use Keboola\JobQueueInternalClient\Result\JobArtifacts;
use PHPUnit\Framework\TestCase;

class ArtifactsTest extends TestCase
{
    public function testSetUploaded(): void
    {
        $artifacts = new JobArtifacts();
        $artifacts->setUploaded([
            new Result(12345),
        ]);

        self::assertSame([
            'uploaded' => [
                [
                    'storageFileId' => '12345',
                ],
            ],
            'downloaded' => [],
        ], $artifacts->jsonSerialize());
    }

    public function testSetDownloaded(): void
    {
        $artifacts = new JobArtifacts();
        $artifacts->setDownloaded([
            new Result(12345),
            new Result(12346),
            new Result(12347),
        ]);

        self::assertSame([
            'uploaded' => [],
            'downloaded' => [
                [
                    'storageFileId' => '12345',
                ],
                [
                    'storageFileId' => '12346',
                ],
                [
                    'storageFileId' => '12347',
                ],
            ],
        ], $artifacts->jsonSerialize());
    }
}

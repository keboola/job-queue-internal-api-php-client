<?php

declare(strict_types=1);

namespace Keboola\JobQueueInternalClient\Tests\Result;

use Keboola\JobQueueInternalClient\Result\Artifacts;
use PHPUnit\Framework\TestCase;

class ArtifactsTest extends TestCase
{
    public function testSetUploaded(): void
    {
        $artifacts = new Artifacts();
        $artifacts->setUploaded([
            'storageFileId' => '12345',
        ]);

        self::assertSame([
            'uploaded' => [
                'storageFileId' => '12345',
            ],
            'downloaded' => null,
        ], $artifacts->jsonSerialize());
    }

    public function testSetDownloaded(): void
    {
        $artifacts = new Artifacts();
        $artifacts->setDownloaded([
            [
                'storageFileId' => '12345',
            ],
            [
                'storageFileId' => '12346',
            ],
            [
                'storageFileId' => '12347',
            ],
        ]);

        self::assertSame([
            'uploaded' => null,
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

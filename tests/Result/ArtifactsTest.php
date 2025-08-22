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

    public function testAddUploaded(): void
    {
        $artifacts = new JobArtifacts();
        $artifacts->setUploaded([
            new Result(100),
        ]);

        $artifacts->addUploaded([
            new Result(200),
            new Result(300),
        ]);

        self::assertSame([
            'uploaded' => [
                ['storageFileId' => '100'],
                ['storageFileId' => '200'],
                ['storageFileId' => '300'],
            ],
            'downloaded' => [],
        ], $artifacts->jsonSerialize());
    }

    public function testAddUploadedToEmpty(): void
    {
        $artifacts = new JobArtifacts();
        $artifacts->addUploaded([
            new Result(400),
        ]);

        self::assertSame([
            'uploaded' => [
                ['storageFileId' => '400'],
            ],
            'downloaded' => [],
        ], $artifacts->jsonSerialize());
    }

    public function testAddDownloaded(): void
    {
        $artifacts = new JobArtifacts();
        $artifacts->setDownloaded([
            new Result(500),
        ]);

        $artifacts->addDownloaded([
            new Result(600),
            new Result(700),
        ]);

        self::assertSame([
            'uploaded' => [],
            'downloaded' => [
                ['storageFileId' => '500'],
                ['storageFileId' => '600'],
                ['storageFileId' => '700'],
            ],
        ], $artifacts->jsonSerialize());
    }

    public function testAddDownloadedToEmpty(): void
    {
        $artifacts = new JobArtifacts();
        $artifacts->addDownloaded([
            new Result(800),
        ]);

        self::assertSame([
            'uploaded' => [],
            'downloaded' => [
                ['storageFileId' => '800'],
            ],
        ], $artifacts->jsonSerialize());
    }
}

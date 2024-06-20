<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require dirname(__DIR__).'/vendor/autoload.php';

if (file_exists(dirname(__DIR__).'/.env')) {
    (new Dotenv())->usePutenv()->bootEnv(dirname(__DIR__) . '/.env', 'dev', []);
}

$requiredEnvs = [
    'TEST_HOSTNAME_SUFFIX', 'TEST_QUEUE_API_TOKEN', 'TEST_STORAGE_API_TOKEN', 'TEST_STORAGE_API_TOKEN_MASTER',
    'TEST_KMS_KEY_ID', 'TEST_KMS_REGION', 'TEST_AWS_ACCESS_KEY_ID', 'TEST_AWS_SECRET_ACCESS_KEY',
    'TEST_AZURE_CLIENT_ID', 'TEST_AZURE_CLIENT_SECRET', 'TEST_AZURE_TENANT_ID', 'TEST_AZURE_KEY_VAULT_URL',
    'TEST_GCP_KMS_KEY_ID', 'TEST_GOOGLE_APPLICATION_CREDENTIALS',
];
foreach ($requiredEnvs as $env) {
    if (empty(getenv($env))) {
        throw new Exception(sprintf('Environment variable "%s" is empty', $env));
    }
}

(new Dotenv())->usePutenv()->populate([
    'TEST_QUEUE_API_URL' => 'https://queue.' . getenv('TEST_HOSTNAME_SUFFIX'),
    'TEST_STORAGE_API_URL' => 'https://connection.' . getenv('TEST_HOSTNAME_SUFFIX'),
]);

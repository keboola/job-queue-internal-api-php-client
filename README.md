# Job Queue Internal API PHP Client [![Build Status](https://dev.azure.com/keboola-dev/job-queue-internal-api-php-client/_apis/build/status/keboola.job-queue-internal-api-php-client?branchName=master)](https://dev.azure.com/keboola-dev/job-queue-internal-api-php-client/_build/latest?definitionId=3&branchName=master)

PHP client for the Internal Job Queue API ([API docs](https://app.swaggerhub.com/apis-docs/keboola/job-queue-internal-api/1.0.1)).

## Usage
```bash
composer require keboola/job-queue-internal-api-php-client
```

```php
use Keboola\JobQueueInternalClient\Client;


$storageClientFactory = new JobFactory\StorageClientFactory('http://connetion.keboola.com/');
$objectEncryptorFactory = new ObjectEncryptorFactory('alias/some-key', 'us-east-1', '', '');
$jobFactory = new JobFactory($storageClientFactory, $objectEncryptorFactory);
$client = new Client(
    new NullLogger(),
    $jobFacory,
    'http://internal.queue.api/',
    'testQueueToken'
);
$client->getJobData('123');
$client->postJobResult('123', 'success', ['images' => ['digests' => []]);
```

## Development
Create a user (`JobQueueInternalApiPhpClient`) for local development using the `test-cf-stack.json` CF template. Create AWS key for the created user. Set the following environment variables in `.env` file (use `.env.dist` as sample):

- `AWS_ACCESS_KEY_ID` - The created security credentials for the `JobQueueInternalApiPhpClient` user.
- `AWS_SECRET_ACCESS_KEY` - The created security credentials for the `JobQueueInternalApiPhpClient` user.
- `TEST_KMS_REGION` - `Region` output of the above stack.
- `TEST_KMS_KEY_ALIAS` - `KmsKey` output of the above stack.
- `TEST_STORAGE_API_URL` - Keboola Connection URL.
- `TEST_STORAGE_API_TOKEN` - Token a to test project.

Than you can run tests:

    docker-compose build
    docker-compose run tests

To run tests with local code use:

    docker-compose run tests-local composer install
    docker-compose run tests-local


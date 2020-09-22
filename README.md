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
Create a service principal to download Internal Queue API image and login:

	SERVICE_PRINCIPAL_NAME=devel-job-queue-internal-api-pull

	ACR_REGISTRY_ID=$(az acr show --name keboolapes --query id --output tsv --subscription c5182964-8dca-42c8-a77a-fa2a3c6946ea)

	SP_PASSWORD=$(az ad sp create-for-rbac --name http://$SERVICE_PRINCIPAL_NAME --scopes $ACR_REGISTRY_ID --role acrpull --query password --output tsv)
	
	SP_APP_ID=$(az ad sp show --id http://$SERVICE_PRINCIPAL_NAME --query appId --output tsv)

	SP_APP_ID=$(az ad sp show --id http://$SERVICE_PRINCIPAL_NAME --query password --output tsv)

Login and pull the image:

	docker login keboolapes.azurecr.io --username $SP_APP_ID --password $SP_PASSWORD

	docker pull keboolapes.azurecr.io/job-queue-internal-api:latest

Create a user (`JobQueueInternalApiPhpClient`) for local development using the `test-cf-stack.json` CF template. Create AWS key for the created user. Set the following environment variables in `.env` file (use `.env.dist` as sample):

- `AWS_ACCESS_KEY_ID` - The created security credentials for the `JobQueueInternalApiPhpClient` user.
- `AWS_SECRET_ACCESS_KEY` - The created security credentials for the `JobQueueInternalApiPhpClient` user.
- `TEST_KMS_REGION` - `Region` output of the above stack.
- `TEST_KMS_KEY_ALIAS` - `KmsKey` output of the above stack.
- `TEST_STORAGE_API_URL` - Keboola Connection URL.
- `TEST_STORAGE_API_TOKEN` - Token a to test project.

Than you can run tests:

```bash
    docker-compose build
    docker-compose run tests
```

To run tests with local code use:

```bash
    docker-compose run tests-local composer install
    docker-compose run tests-local
```

# Job Queue Internal API PHP Client [![Build Status](https://dev.azure.com/keboola-dev/job-queue-internal-api-php-client/_apis/build/status/keboola.job-queue-internal-api-php-client?branchName=master)](https://dev.azure.com/keboola-dev/job-queue-internal-api-php-client/_build/latest?definitionId=3&branchName=master)

PHP client for the Internal Job Queue API.

## Usage
```bash
composer require keboola/job-queue-internal-api-php-client
```


```php
use Keboola\JobQueueInternalClient\Client;

$client = new Client('http://internal.api/', 'testToken');
$client->getJobData('123');
$client->postJobResult('123', ['images' => ['digests' => []]);
```

## Development
Run tests with `docker-compose up`

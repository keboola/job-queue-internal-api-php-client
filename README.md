# Job Queue Internal API PHP Client [![Build Status](https://dev.azure.com/keboola-dev/job-queue-internal-api-php-client/_apis/build/status/keboola.job-queue-internal-api-php-client?branchName=main)](https://dev.azure.com/keboola-dev/job-queue-internal-api-php-client/_build/latest?definitionId=3&branchName=main)

PHP client for the Internal Job Queue API ([API docs](https://app.swaggerhub.com/apis-docs/keboola/job-queue-internal-api/)).

## Usage
```bash
composer require keboola/job-queue-internal-api-php-client
```

```php
use Keboola\JobQueueInternalClient\Client;

$storageClientFactory = new JobFactory\StorageClientFactory('http://connetion.keboola.com/');
$objectEncryptorFactory = new ObjectEncryptorFactory('key-id', 'us-east-1', '', '');
$jobFactory = new JobFactory($storageClientFactory, $objectEncryptorFactory);
$client = new Client(
    new NullLogger(),
    $jobFactory,
    'http://internal.queue.api/',
    'testQueueToken'
);
$client->getJobData('123');
$client->postJobResult('123', 'success', ['images' => ['digests' => []]]);
```

## Development
Prerequisites:
* configured `az` and `aws` CLI tools (run `az login` and `aws configure --profile keboola-dev-platform-services`)
* installed `terraform` (https://www.terraform.io) and `jq` (https://stedolan.github.io/jq) to setup local env
* intalled `docker` and `docker compose` to run & develop the app

TL;DR:
```
export NAME_PREFIX= # your name/nickname to make your resource unique & recognizable

cat <<EOF > ./provisioning/local/terraform.tfvars
name_prefix = "${NAME_PREFIX}"
EOF

cat <<EOF > .env.local
TEST_STORAGE_API_URL=https://connection.keboola.com
TEST_STORAGE_API_TOKEN=
TEST_STORAGE_API_TOKEN_MASTER=
EOF

terraform -chdir=./provisioning/local init -backend-config="key=job-queue-internal-api-php-client/${NAME_PREFIX}.tfstate"
terraform -chdir=./provisioning/local apply
./provisioning/local/update-env.sh azure # or aws
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.

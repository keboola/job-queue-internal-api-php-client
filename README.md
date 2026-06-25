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

### Atomic job result updates

`patchJobResultAtomically()` performs an optimistic-locking read-modify-write of a job's
`result` document. It reads the current result and its `resultVersion`, runs your mutator,
and writes the full replacement back guarded by that version. On a version conflict (HTTP 409,
i.e. a concurrent writer changed the result in between) it automatically re-reads and retries a
bounded number of times; if the conflict still cannot be resolved it falls back to a legacy
server-side merge write so a terminal write always lands.

The mutator receives the current result array and must return a `\JsonSerializable` whose
`jsonSerialize()` yields an array — the **full** replacement document (the versioned write is a
full replace, not a merge), so always round-trip the whole result:

```php
use Keboola\JobQueueInternalClient\Result\GenericJobResult;

$job = $client->patchJobResultAtomically('123', function (array $current): GenericJobResult {
    $current['processedRows'] = ($current['processedRows'] ?? 0) + 1;
    return new GenericJobResult($current);
});
```

`GenericJobResult` is a trivial `\JsonSerializable` wrapper for callers that do not have their
own result value object.

## Development
Prerequisites:
* configured `az` and `aws` CLI tools (run `az login` and `aws configure --profile keboola-dev-platform-services`)
* installed GCP CLI `gcloud` (and run `gcloud auth login` or `gcloud auth application-default login`)
* installed `terraform` (https://www.terraform.io) and `jq` (https://stedolan.github.io/jq) to setup local env
* intalled `docker` and `docker compose` to run & develop the app

TL;DR:
```
export NAME_PREFIX= # your name/nickname to make your resource unique & recognizable

cat <<EOF > ./provisioning/local/terraform.tfvars
name_prefix = "${NAME_PREFIX}"
EOF

cat <<EOF > .env.local
TEST_HOSTNAME_SUFFIX=keboola.com
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

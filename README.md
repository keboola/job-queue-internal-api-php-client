# Job Queue Internal API PHP Client [![Build Status](https://dev.azure.com/keboola-dev/job-queue-internal-api-php-client/_apis/build/status/keboola.job-queue-internal-api-php-client?branchName=master)](https://dev.azure.com/keboola-dev/job-queue-internal-api-php-client/_build/latest?definitionId=3&branchName=master)

PHP client for the Internal Job Queue API ([API docs](https://app.swaggerhub.com/apis-docs/keboola/job-queue-internal-api/1.0.1)).

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
- Create a service principal to download Internal Queue API image and login:

    ```bash
        SERVICE_PRINCIPAL_NAME=[USERNAME]-job-queue-internal-api-pull
        ACR_REGISTRY_ID=$(az acr show --name keboolapes --query id --output tsv --subscription c5182964-8dca-42c8-a77a-fa2a3c6946ea)
        SP_PASSWORD=$(az ad sp create-for-rbac --name http://$SERVICE_PRINCIPAL_NAME --scopes $ACR_REGISTRY_ID --role acrpull --query password --output tsv)
        SP_APP_ID=$(az ad sp show --id http://$SERVICE_PRINCIPAL_NAME --query appId --output tsv)    
    ```

- Login and pull the image:

    ```bash
        docker login keboolapes.azurecr.io --username $SP_APP_ID --password $SP_PASSWORD
        docker pull keboolapes.azurecr.io/job-queue-internal-api:latest
    ```

- Set the following environment variables in `.env` file (use `.env.dist` as sample):
    - `TEST_STORAGE_API_URL` - Keboola Connection URL.
    - `TEST_STORAGE_API_TOKEN` - Token to a test project.
  
### AWS Setup
- Create a user (`JobQueueInternalApiPhpClient`) for local development using the `provisioning/aws.json` CF template. 
    - Create AWS key for the created user. 
    - Set the following environment variables in `.env` file (use `.env.dist` as sample):
        - `TEST_AWS_ACCESS_KEY_ID` - The created security credentials for the `JobQueueInternalApiPhpClient` user.
        - `TEST_AWS_SECRET_ACCESS_KEY` - The created security credentials for the `JobQueueInternalApiPhpClient` user.
        - `TEST_KMS_REGION` - `Region` output of the above stack.
        - `TEST_KMS_KEY_ID` - `KmsKey` output of the above stack.

### Azure Setup

- Create a resource group:
    ```bash
    az account set --subscription "Keboola DEV PS Team CI"
    az group create --name testing-job-queue-internal-api-php-client --location "East US"
    ```

- Create a service principal:
    ```bash
    az ad sp create-for-rbac --name testing-job-queue-internal-api-php-client
    ```

- Use the response to set values `TEST_AZURE_CLIENT_ID`, `TEST_AZURE_CLIENT_SECRET` and `TEST_AZURE_TENANT_ID` in the `.env.` file:
    ```json 
    {
      "appId": "268a6f05-xxxxxxxxxxxxxxxxxxxxxxxxxxx", //-> TEST_CLIENT_ID
      "displayName": "testing-job-queue-internal-api-php-client",
      "name": "http://testing-job-queue-internal-api-php-client",
      "password": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", //-> TEST_CLIENT_SECRET
      "tenant": "9b85ee6f-xxxxxxxxxxxxxxxxxxxxxxxxxxx" //-> TEST_TENANT_ID
    }
    ```

- Get ID of the service principal:
    ```bash
    SERVICE_PRINCIPAL_ID=$(az ad sp list --display-name testing-job-queue-internal-api-php-client --query "[0].objectId" --output tsv)
    ```

- Get ID of a group to which the current user belongs (e.g. "Developers"):
    ```bash
    GROUP_ID=$(az ad group list --display-name "Developers" --query "[0].objectId")
    ```

- Deploy the key vault, provide tenant ID, service principal ID and group ID from the previous commands:
    ```bash
    az deployment group create --resource-group testing-job-queue-internal-api-php-client --template-file provisioning/azure.json --parameters vault_name=testing-job-queue-internal-api-php-client tenant_id=9b85ee6f-4fb0-4a46-8cb7-4dcc6b262a89 service_principal_object_id=$SERVICE_PRINCIPAL_ID group_object_id=$GROUP_ID
    ```
  returns e.g. `https://testing-key-vault-client.vault.azure.net/keys/test-key/b7c28xxxxxxxxxxxxxxxxxxxxxxxxxxx`, use this to set values in `.env` file:
    - `TEST_AZURE_KEY_VAULT_URL` - https://testing-key-vault-client.vault.azure.net

## Run tests
- With the above setup, you can run tests:

    ```bash
    docker-compose build
    docker-compose run tests
    ```

- To run tests with local code use:

    ```bash
    docker-compose run tests-local composer install
    docker-compose run tests-local
    ```

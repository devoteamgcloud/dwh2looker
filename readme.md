# dwh2looker
dwh2looker automatically generates your LookML views from your database models.

> **Note:** Currently, `dwh2looker` is optimized for **Google BigQuery**. Support for other data warehouses is under consideration for future releases.

dwh2looker has 3 features:
- LookML generator: this is the main feature, that reads the table schemas from database and creates the LookML base views
- [Optional] Track differences of models between tables in two distinct schemas in the same database, which are meant to correspond to your development and production schemas, in order to identify which models need to be synced.
- [Optional] Commit the LookML view files to your Looker repository (only supports GitHub at the moment)

Assumptions:
- The fields in your tables have descriptions (supported by some databases only). If you use dbt, you can use `persist-docs` to store the fields descriptions in the database (see [dbt docs](https://docs.getdbt.com/reference/resource-configs/persist_docs)).
- Your Looker project is structured with base, standard and logical layers, following [this approach](https://www.spectacles.dev/post/fix-your-lookml-project-structure). dwh2looker helps you create and update your base layer, without any measures.

## Installation

You can install dwh2looker from a PyPi repository. We suggest you install it into a virtual environment.
Please specify which database (or databases) you will be using, as in the example below.

```shell
pip install "dwh2looker[bigquery]"
```

## Configuration

`dwh2looker` is configured through a central `config.json` file and environment variables.

1.  **Create your configuration file.**
    Start by copying the provided example file:
    ```bash
    cp config.example.json config.json
    ```
    Then, edit `config.json` to match your project's settings (database connections, Looker repo details, etc.).

2.  **Set up your environment.**
    The application needs to know where your configuration file is located.
    - For local development, copy the `.env.example` file to `.env`:
      ```bash
      cp .env.example .env
      ```
    - Then, update `.env` to ensure `dwh2looker_CONFIG_FILE` points to your `config.json`. The `dwh2looker` application will automatically load this file.
    - For production or CI/CD environments, set the `dwh2looker_CONFIG_FILE` environment variable directly.

3.  **Authentication.**
    Ensure you are authenticated with your data warehouse. For Google BigQuery, you can connect via OAuth (by running `gcloud auth application-default login`) or by providing a service account in your `config.json`.

### Config file

The config file allows you to customise your LookML views. It supports the following options:

- `hide_all_fields`: Defaults to false. If set to true, all fields in the LookML view will be hidden in Looker.

- `timeframes`: The list of timeframes to use for your time dimension group fields. It uses the following timeframes as default:
    ```json
    "timeframes": [
      "raw",
      "time",
      "date",
      "day_of_year",
      "week",
      "week_of_year",
      "month",
      "month_name",
      "month_num",
      "quarter",
      "year"
    ]
    ```

- `capitalize_ids`: Defaults to False. If your field name contains "Id", it will be replaced by "ID". You can set it to true if you prefer.

- `primary_key_prefixes`: List of prefixes for field names to be assumed to be a primary key in Looker. E.g: ` "primary_key_prefixes": ["pk_"]`.

- `ignore_column_types`: List of database field types to be ignored on the LookML creation. E.g: `"ignore_column_types": ["GEOGRAPHY", "JSON"]`

- `ignore_modes`: List of database field modes that will be ignored on the LookML creation. E.g: `"ignore_modes": []`

- `time_suffixes`: List of suffixes on your database field names to be omitted in the Looker field names. E.g. `"time_suffixes": ["_date", "_time", "_timestamp", "_datetime"]` will mean that the field `created_date` will be named `created` in Looker.

- `dimension_groups_excluded`: A list of strings specifying which time-based fields should be created as a standard `dimension` instead of a `dimension_group`. E.g.: `"dimension_groups_excluded": [
    "valid_from",
    "valid_to",
    "load_",
    "pk_",
    "week_",
    "holiday_",
    "fk_"
  ]`

- `looker_repo_structure`: Defines the structure and details of your Looker GitHub repository for automated LookML deployment. This includes:
    - `repo_url`: The URL of your Looker GitHub repository.
    - `main_branch`: The main branch of your repository (e.g., "main", "master").
    - `branch_name`: The name of the branch to which new LookML views will be committed.
    - `base_views`: The directory path within the repository where base LookML views will be stored.
    - `refined_views`: The directory path within the repository where refined LookML views will be stored.
    - `base_explores`: The directory path within the repository where base LookML explores will be stored.
    - `github_user_email`: The GitHub user email to be used for commits.

- `tables_env`: A list of environments and their corresponding dataset and project IDs. This allows you to configure different database environments (e.g., development, production) for generating LookML. Each entry in the list should have:
    - `env`: The name of the environment (e.g., "cprod", "prod").
    - `dataset_id`: The dataset ID for that environment.
    - `project_id`: The project ID for that environment.
    - `credentials_path` (optional): The path to a GCP credentials file. This can be a service account key or a Workload Identity configuration file. If omitted, the application will use Application Default Credentials (ADC), which is suitable for local development with `gcloud` or environments like GitHub Actions where authentication is managed automatically.
    - `exclude_tables` (optional): A list of table names to be excluded from generating LookML in this environment. E.g., `["table1", "table2"]`.

Example `tables_env` configuration for Workload Identity in GitHub Actions:
```json
"tables_env": [
  {
    "env": "cprod",
    "dataset_id": "dataset_id",
    "project_id": "project_id",
    "credentials_path": "./gcp_creds_cprod.json"
  },
  {
    "env": "prod",
    "dataset_id": "dataset_id",
    "project_id": "project_id",
    "credentials_path": "./gcp_creds_prod.json"
  }
]
```

```json
{
  "primary_key_prefixes": ["pk_"],
  "ignore_column_types": ["GEOGRAPHY", "JSON"],
  "ignore_modes": [],
  "timeframes": [
    "raw",
    "time",
    "date",
    "day_of_year",
    "week",
    "week_of_year",
    "month",
    "month_name",
    "month_num",
    "quarter",
    "year"
  ],
  "capitalize_ids": false,
  "time_suffixes": ["_date", "_time", "_timestamp", "_datetime"],
  "dimension_groups_excluded": [
    "valid_from",
    "valid_to",
    "load_",
    "pk_",
    "week_",
    "holiday_",
    "fk_"
  ],
  "looker_repo_structure": {
    "repo_url": "repo_url",
    "main_branch": "main",
    "branch_name": "auto_generated_base_views",
    "base_views": "base_views",
    "refined_views": "refined_views",
    "github_user_email": "github_user_email"
  },
  "tables_env": [
    {
      "env": "cprod",
      "dataset_id": "dataset_id",
      "project_id": "project_id",
      "credentials_path": "./gcp_creds_cprod.json"
    },
    {
      "env": "prod",
      "dataset_id": "dataset_id",
      "project_id": "project_id",
      "credentials_path": "./gcp_creds_prod.json"
    }
  ]
}
```


## Commands

### Diff Tracker

```bash
dwh2looker diff_tracker [options]
```

Use this command if you need to compare models between 2 different schemas in a database, supposed to correspond to your dev and prod targets. This command will return a list of the models which are different, ie, the ones you will need to update. This can be particulary useful when developping on your models, to know which models you will need to refresh in Looker.

#### Arguments

- `--db_type` (type: str, required: True): Database type (bigquery, redshift, snowflake).

- `--dataset1_name` (type: str, required: True): Name of Dataset 1.

- `--dataset2_name` (type: str, required: True): Name of Dataset 2.

- `--project` (type: str, required: True): Project ID.

- `--service_account` (type: str, required: False): Google Service Account.

- `--models` (type: str): List of model names to compare (comma-separated) or file path with a model per line. You can pass your dbt marts only, for example.

- `--output` (type: str): Output file path to write the results to.

- `--full-refresh` (action: Boolean, default: False): If you want to perform a full refresh of all models. This will return all models inputed (so this step does not run). This is only useful if you want to skip this command when refreshing all models, for example in a CI pipeline.

#### Example
```bash
dwh2looker diff_tracker \
    --db_type bigquery \
    --project my-database-name \
    --dataset1_name dbt_dev \
    --dataset2_name dbt_prod \
    --models tmp/marts.txt \
    --output tmp/diff.txt
```

### Generate LookML

```bash
dwh2looker generate_lookml [options]
```

This command created the LookML base views.

#### Arguments

- `--db_type` (type: str, required: True): Database type (bigquery, redshift, snowflake).

- `--override-dataset-id` (type: str, required: False): Override Dataset ID. For example, you may be developping and reading from a dev dataset, but you may want to create the LookML views pointing to your production dataset. Or you may have defined a constant in Looker for you dataset name
    ```lookml
    constant: dataset {
        value: "dbt_prod"
    }
    ```
    and set this option value to @{dataset} as in Example 2.

- `--token` (type: str, required: False): GitHub Token.
- `--github-app` (action: Boolean, default: False): Run as GitHub App (omit this flag if running with PAT).
- `--push-lookml-to-looker` (action: Boolean, default: False): Push generated LookML to Looker via GitHub.
- `--draft-pr` (action: Boolean, default: False): Create the Pull Request as a Draft.


#### Examples

Example 1:
```bash
dwh2looker generate_lookml \
    --db_type bigquery \
    --override-dataset-id dbt_prod \
    --push-lookml-to-looker \
    --token $GH_TOKEN
```

Example 2:
```bash
dwh2looker generate_lookml \
    --db_type snowflake \
    --override-dataset-id @{dataset}
```

## How to contribute

We welcome contributions to dwh2looker! Please see our [Contributing Guide](CONTRIBUTING.md) for details on how to set up your development environment, run tests, and submit pull requests.

---

dwh2looker was inspired by Optician

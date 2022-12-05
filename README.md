# dbt-snowflake-monitoring

A dbt package to help you monitor Snowflake performance and costs. Documentation for the models can be found [here](https://get-select.github.io/dbt-snowflake-monitoring/#!/overview).

## Quickstart

Grant dbt's role access to the `snowflake` database:

```sql
grant imported privileges on database snowflake to role your_dbt_role_name;
```

Add the following to your `packages.yml` file:

```yaml
packages:
  - package: get-select/dbt_snowflake_monitoring
    version: 1.2.2
```

To attribute costs to individual models via the `dbt_metadata` column in the `query_history_enriched` model, add the following to `dbt_project.yml`:

```yaml
query-comment:
  comment: '{{ dbt_snowflake_monitoring.get_query_comment(node) }}'
  append: true # Snowflake removes prefixed comments.
```

## Example Usage

### Sample Queries

See [sample_queries.md](/documentation/sample_queries.md)

## Contributing

### Initial setup

1. Install pipx
```bash
pip install pipx
pipx ensurepath
```

2. Install tox
```bash
pipx install tox
```

3. Install pre-commit
```bash
pipx install pre-commit
pre-commit install
```

4. Configure your profile (follow the prompts)
```
dbt init
```

### Adding a CHANGELOG Entry
We use changie to generate CHANGELOG entries. Note: Do not edit the CHANGELOG.md directly. Your modifications will be lost.

Follow the steps to [install changie](https://changie.dev/guide/installation/) for your system.

Once changie is installed and your PR is created, simply run changie new and changie will walk you through the process of creating a changelog entry. Commit the file that's created and your changelog entry is complete!

### Developing the package

Simply treat this package like a dbt project. From the top level of the repo, you can run:
```
dbt build
```

and any other dbt command.

### SQLFluff

We use SQLFluff to keep SQL style consistent. By installing `pre-commit` per the initial setup guide above, SQLFluff will run automatically when you make a commit locally. A GitHub action automatically tests pull requests and adds annotations where there are failures.

Lint all models in the /models directory:
```bash
tox -e lint_all
```

Fix all models in the /models directory:
```bash
tox -e fix_all
```

Lint (or subsitute lint to fix) a specific model:
```bash
tox -e lint -- models/path/to/model.sql
```

Lint (or subsitute lint to fix) a specific directory:
```bash
tox -e lint -- models/path/to/directory
```

#### Rules

Enforced rules are defined within `tox.ini`. To view the full list of available rules and their configuration, see the [SQLFluff documentation](https://docs.sqlfluff.com/en/stable/rules.html).

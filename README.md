# dbt-snowflake-monitoring

A dbt package to help you monitor Snowflake performance and costs. [See the dbt docs site here](https://get-select.github.io/dbt-snowflake-monitoring/#!/overview).

## Installation

Ensure that the Snowflake role used by your dbt project has permission to read the required `snowflake.account_usage` and `snowflake.organization` views. If it does not, you can run the following SQL to grant the required permissions:

```sql
grant imported privileges on database snowflake to role your_dbt_role_name;
```

We are currently in the process of getting this package added to the dbt package hub. In the meantime, you can add it to your package by adding the following to your `packages.yml` file:

```yaml
packages:
  - git: https://github.com/get-select/dbt-snowflake-monitoring.git
    revision: 1.0.0
```

In your dbt project, you can then turn it on/off with the `enabled` property:

```yaml
models:

  ...

  dbt_snowflake_monitoring:
    enabled: true
```

## Example Usage

### Sample Queries

See [sample_queries.md](/documentation/sample_queries.md)

## Query cost caveats

It's important to note that removing a particular query does not guarantee your Snowflake bill to decrease by the cost associated with that query. Snowflake bills based on the number of seconds your warehouse is running. If other queries are running at the same time as the query you removed, you will still be billed for that time.

For the cloud services credits associated with your queries, you are only billed for them if they exceed 10% of your compute credit consumption on a given day. The cost per query model correctly handles this nuance by performing this applicability check prior to adding any cloud service costs to the queries. However, for queries that ran on the current date, this calculation may be innacurate. For example, say the cost per query model runs at 10AM today. For all the queries that occured before 10AM today, it finds that the total cloud service credits consumed was 15, and the total compute credits consumed was 100. Since this is greater than 10%, 5 cloud services credits will be allocated to the queries from today. It's possible that this fraction shifts as more queries run, and at the end of the day we have 90 cloud service credits and 1000 compute credits. If we re-run the model again after the day is completed, the queries were no longer have any cloud service costs associated with them. In the grand scheme of things, this won't make any significant difference in your analysis, but it is worth calling out.

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

### Developing the package

Simply treat this package like a dbt project. From the top level of the repo, you can run:
```
dbt build
```

and any other dbt command.

To run the tests:
```
tox
```

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

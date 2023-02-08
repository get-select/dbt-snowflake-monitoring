# dbt-snowflake-monitoring

From the [SELECT](https://select.dev) team, a dbt package to help you monitor Snowflake performance and costs. Documentation for the models can be found [here](https://get-select.github.io/dbt-snowflake-monitoring/#!/overview).

## Quickstart

Grant dbt's role access to the `snowflake` database:

```sql
grant imported privileges on database snowflake to role your_dbt_role_name;
```

Add the following to your `packages.yml` file:

```yaml
packages:
  - package: get-select/dbt_snowflake_monitoring
    version: 2.0.0
```

To attribute costs to individual models via the `dbt_metadata` column in the `query_history_enriched` model, create a new macro named `query_tags.sql` in your `macros` folder. Add the following to the file:

```sql
{% macro set_query_tag() -%}
{% do return(dbt_snowflake_monitoring.set_query_tag()) %}
{% endmacro %}

{% macro unset_query_tag(original_query_tag) -%}
{% do return(dbt_snowflake_monitoring.unset_query_tag(original_query_tag)) %}
{% endmacro %}
```

To generate URLs to dbt Cloud jobs and runs in the `dbt_queries` model, add the following variable to `dbt_project.yml`:
```yaml
vars:
  dbt_cloud_account_id: 12345 # https://cloud.getdbt.com/deploy/<this_number>/projects/<not_this_number>/jobs
```

### Only want to use the get_query_comment macro?

If you only want to use the `get_query_comment` macro, and don't want to run the models, you can exclude them from running by adding the following to your `dbt_project.yml`:

```yaml
models:
  dbt_snowflake_monitoring:
    +enabled: false
```

## Package Alternatives & Maintenance

Prior to releasing this package, [snowflake-spend](https://gitlab.com/gitlab-data/snowflake_spend) by the Gitlab data team was the only package available for monitoring Snowflake spend. According to their README, the package is currently maintained by the Gitlab data team, but there does not appear to be any active development in it (as of January 2023).

The `dbt-snowflake-monitoring` package is actively developed & maintained by the [SELECT](https://select.dev/) team. The package goes beyond modeling warehouse spend - it calculates cost per query using the methodology described [here](https://select.dev/posts/cost-per-query) and all billable Snowflake components. Additional query & warehouse performance related models will be added in the coming weeks.

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

Once changie is installed and your PR is created, simply run `changie new` and changie will walk you through the process of creating a changelog entry. Commit the file that's created and your changelog entry is complete!

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

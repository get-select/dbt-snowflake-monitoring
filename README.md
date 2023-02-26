# dbt-snowflake-monitoring

From the [SELECT](https://select.dev) team, a dbt package to help you monitor Snowflake performance and costs. Documentation for the models can be found [here](https://get-select.github.io/dbt-snowflake-monitoring/#!/overview).

## Quickstart

1. Grant dbt's role access to the `snowflake` database:

```sql
grant imported privileges on database snowflake to role your_dbt_role_name;
```

2. Add the package to your `packages.yml` file:

```yaml
packages:
  - package: get-select/dbt_snowflake_monitoring
    version: 3.0.0
```

3. To attribute costs to individual models via the `dbt_metadata` column in the `query_history_enriched` model, query comments and tags are added to all dbt-issued queries. Both query comments and tags are needed to collect the required metadata for the `dbt_queries` model.

To add the query tags follow one of these two options:

Option 1: If running dbt < 1.2, create a folder named `macros` in your dbt project's top level directory (if it doesn't exist). Inside, make a new file called `query_tags.sql` with the following content:

```sql
{% macro set_query_tag() -%}
{% do return(dbt_snowflake_monitoring.set_query_tag()) %}
{% endmacro %}

{% macro unset_query_tag(original_query_tag) -%}
{% do return(dbt_snowflake_monitoring.unset_query_tag(original_query_tag)) %}
{% endmacro %}
```

Option 2: If running dbt >= 1.2, simply configure the dispatch search order in `dbt_project.yml`.

```yaml
dispatch:
  - macro_namespace: dbt
    search_order:
      - <YOUR_PROJECT_NAME>
      - dbt_snowflake_monitoring
      - dbt
```

4. To configure the query comments, add the following config to `dbt_project.yml`.

```yaml
query-comment:
  comment: '{{ dbt_snowflake_monitoring.get_query_comment(node) }}'
  append: true # Snowflake removes prefixed comments.
```

That's it! All dbt-issued queries will now be tagged and start appearing in the `dbt_queries.sql` model.

### dbt Cloud URLs

If you're using dbt Cloud, columns `dbt_cloud_job_url` and `dbt_cloud_run_url` can be configured in the `dbt_queries` model. To do so, set the variable `dbt_cloud_account_id`. This id can be retrieved from in between `/deploy/` and `/projects/` in any dbt Cloud URL.

By default, the URL prefix of `https://cloud.getdbt.com/deploy/` is used. If you're using a different region of dbt Cloud, this prefix can be overridden by specifying the `dbt_cloud_url` variable.

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

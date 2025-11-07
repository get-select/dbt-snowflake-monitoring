# [dbt-snowflake-monitoring](https://select.dev/docs/dbt-snowflake-monitoring)

From the [SELECT](https://select.dev) team, a dbt package to help you monitor Snowflake performance and costs.

## Documentation

Documentation for the package resides on the SELECT website for greater rendering flexibility. For questions and support, please either create an issue or reach out via the Intercom chat bubble on the website 🙂

* [Setup instructions and example queries](https://select.dev/docs/resources/dbt-snowflake-monitoring)
* [Generated dbt Docs for the package's resources](https://get-select.github.io/dbt-snowflake-monitoring/#!/overview)


## Configuration Variables

This package supports the following optional variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `snowflake_account_usage_database` | Database for account_usage schema | `snowflake` |
| `snowflake_account_usage_schema` | Schema for account usage data | `account_usage` |
| `snowflake_organization_usage_database` | Database for organization_usage schema | `snowflake` |
| `snowflake_organization_usage_schema` | Schema for organization usage data | `organization_usage` |


Point your project to your custom database with the following defined in your own `dbt_project.yml`:
```yaml
vars:
  snowflake_account_usage_database: "my_custom_db"
  snowflake_account_usage_schema: "custom_account_usage"
  snowflake_organization_usage_database: "my_custom_db"
  snowflake_organization_usage_schema: "custom_organization_usage"
```
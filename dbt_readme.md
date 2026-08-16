# dbt Initialization

This project runs dbt inside Docker so Airflow can orchestrate it later.

## 1. Add Databricks credentials

Create a local `.env` file from `.env.example` and fill:

```env
DATABRICKS_HOST=
DATABRICKS_HTTP_PATH=
DATABRICKS_TOKEN=
```

## 2. Build the dbt image

```powershell
docker compose --profile tools build dbt
```

## 3. Check the dbt connection

```powershell
docker compose --profile tools run --rm dbt dbt debug
```

If this passes, dbt can connect to Databricks.

## 4. Run dbt

After models are added:

```powershell
docker compose --profile tools run --rm dbt dbt run
docker compose --profile tools run --rm dbt dbt test
```

The dbt project lives in `dbt_upi_fraud/`, and the Databricks profile lives in `profiles/profiles.yml`.

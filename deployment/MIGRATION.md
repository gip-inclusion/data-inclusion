**The goal**: Migrate PostgreSQL from 17 to 18 (datawarehouse) and from 14 to 18 (airflow-db)

### Datawarehouse (PG 17 → 18)

1. **Connect via SSH to the instance**
    ```bash
    ssh root@<INSTANCE_IP>
    cd data-inclusion
    ```

2. **Stop everything, then start only datawarehouse for the dump**
    ```bash
    docker compose stop
    docker compose up datawarehouse -d
    docker compose exec datawarehouse pg_isready
    ```

3. **Dump the datawarehouse**
    ```bash
    pg_dump \
      -d 'postgresql://<user>:<password>@172.17.0.1:5432/<dbname>' \
      --no-owner --no-acl -Fc -v \
      -f /root/migrate_17_to_18.dump
    ```

4. **Stop all services**
    ```bash
    docker compose stop
    ```

5. **Remove the datawarehouse data volume**
    ```bash
    docker volume rm data-inclusion_datawarehouse-data
    ```

6. **Pull the new images**
    ```bash
    docker compose pull
    ```

7. **Start only the datawarehouse to initialize PG 18**
    ```bash
    docker compose up datawarehouse -d
    sleep 10
    docker compose exec datawarehouse pg_isready
    ```

8. **Restore the dump**
    ```bash
    pg_restore \
      --no-owner -Fc \
      -d 'postgresql://<user>:<password>@172.17.0.1:5432/<dbname>' \
      -j 8 -v \
      /root/migrate_17_to_18.dump
    ```

9. **Stop datawarehouse**
    ```bash
    docker compose stop
    ```

### Airflow DB (PG 14 → 18)

All connections and variables are set via environment variables.
Just destroy the old container and its anonymous volume:

```bash
docker compose rm -v airflow-db
```

`airflow-init` will recreate the schema on the new PG 18 image via `_AIRFLOW_DB_MIGRATE`.

### Full restart

```bash
docker compose --progress=plain up \
  --pull=always \
  --force-recreate \
  --remove-orphans \
  --wait \
  --wait-timeout 1200 \
  --quiet-pull \
  --detach
```

### Validation

```bash
docker compose exec datawarehouse psql -U <user> -d <dbname> -c "SELECT version();"
docker compose exec airflow-db psql -U airflow -d airflow -c "SELECT version();"
docker compose exec datawarehouse psql -U <user> -d <dbname> -c "SELECT extname, extversion FROM pg_extension;"
```

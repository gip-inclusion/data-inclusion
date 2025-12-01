Here is the corrected and formatted version of your migration process:
**The goal**: Migrate PostgreSQL from 14 to 17

### Steps:

1. **Connect via SSH to the instance**
```bash
   ssh root@<INSTANCE_IP>
```
2. **Install PostgreSQL 17**
    ```bash
    sudo apt install curl ca-certificates
    sudo install -d /usr/share/postgresql-common/pgdg
    sudo curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc

    sudo sh -c 'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

    sudo apt update

    sudo apt -y install postgresql
    ```

3. **Create a backup of the current database after stoping other container | Stop all **
    ```bash
    cd data-inclusion
    docker compose stop
    docker compose up datawarehouse -d
    pg_dump -d 'postgresql://data_inclusion:{password}@172.17.0.1:5432/data_inclusion' --no-owner --no-acl -Fc -v -f /root/migrate_14_to_17.dump
    ```

4. **Import the dump locally**
   Transfer the backup file to your local machine:

    ```bash
    scp root@163.172.186.56:/root/migrate_14_to_17.dump ~/data
    ```

5. **Stop the instance**
   Stop the running services using Docker Compose:

    ```bash
    docker compose stop
    ```

6. **Clean the data folder**
   Remove all the existing data in the target database container:

    ```bash
    docker compose run datawarehouse 'rm -rf /var/lib/postgresql/data/*'
    ```

7. **Update the PostgreSQL version**
   Deploy from pull request

8. **Run the migration**
   Restore the backup to the new PostgreSQL version:

    ```bash
    docker compose stop
    docker compose run datawarehouse 'rm -rf /var/lib/postgresql/data/*'
    pg_restore --no-owner -Fc /root/migrate_14_to_17.dump -d 'postgresql://datainclusion:{password}@172.17.0.1:5432/datainclusion' -j 8 -v
    ```

9. **Deploy relaunch docker**
    ```Bash
    docker compose --progress=plain up --pull=always --force-recreate --remove-orphans --wait --wait-timeout 1200 --quiet-pull --detach
    ```
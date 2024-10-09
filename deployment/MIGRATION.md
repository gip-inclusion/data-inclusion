The idea is to pass migration from one container to the other playing with volume:

1 Deploy PR disabling other services and creating volume to passe on migration
2 go inside container: `docker compose exec datawarehouse bash`
3 Create back-up:
 `pg_dumpall --username=data-inclusion --database=data-inclusion > /var/lib/postgresql/migrate/migrate_14_to_17.sql`
4 Stop instance: `docker compose stop`
5 Move data:
 `docker compose run target-db bash`
 `mkdir /var/lib/postgresql/migrate/old`
 `mv /var/lib/postgresql/data/* /var/lib/postgresql/migrate/old`
6 Deploy PR with docker image on postgres 17
7 Go inside container: `docker compose exec datawarehouse bash`
9 Run migration: `psql -d data-inclusion -U data-inclusion -f var/lib/postgresql/migrate/migrate_14_to_17.sql`
10 Deploy PR cleaning extra volume created for migration and reenabling other services.

To be adapteed on local environement but the idea is the same
Quid des sauvegardes ?

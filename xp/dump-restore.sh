#!/bin/bash

DWH_URL=postgresql://postgres:password@localhost:5432/xp

# pg_dump --format=custom --clean --if-exists --no-owner --no-privileges --table api__structures --file struct.dump
pg_restore --dbname=$DWH_URL --clean --if-exists --no-owner --no-privileges struct.dump

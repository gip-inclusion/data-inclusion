#!/bin/bash
set -e
cat >> "$PGDATA/pg_hba.conf" << 'EOF'

# Scalingo outbound IPs (temporary: remove after migration)
host  data_inclusion  data_inclusion  148.253.96.190/32  scram-sha-256
host  data_inclusion  data_inclusion  148.253.97.14/32  scram-sha-256
EOF

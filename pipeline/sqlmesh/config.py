import os
from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    GatewayConfig,
    PostgresConnectionConfig,
)

config = Config(
    model_defaults=ModelDefaultsConfig(dialect="postgres"),
    gateways={
        "default": GatewayConfig(
            connection=PostgresConnectionConfig(
                # too bad it doesn't allow connection strings through "database" param.
                host=os.getenv("PGHOST"),
                user=os.getenv("PGUSER"),
                password=os.getenv("PGPASSWORD"),
                port=os.getenv("PGPORT"),
                database=os.getenv("PGDATABASE"),
            ),
        ),
    }
)

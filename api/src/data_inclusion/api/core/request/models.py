import uuid

import sqlalchemy as sqla
from sqlalchemy.dialects.postgresql import JSONB, UUID

from data_inclusion.api.core.db import Base

# all fields are nullable or have a default value. These models will only be used to
# query valid data coming from the data pipeline.


class Request(Base):
    id = sqla.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = sqla.Column(
        sqla.DateTime(timezone=True), server_default=sqla.func.now()
    )
    status_code = sqla.Column(sqla.SmallInteger)
    method = sqla.Column(sqla.Text)
    path = sqla.Column(sqla.Text)
    base_url = sqla.Column(sqla.Text)
    user = sqla.Column(sqla.Text, nullable=True)
    path_params = sqla.Column(JSONB)
    query_params = sqla.Column(JSONB)
    client_host = sqla.Column(sqla.Text)
    client_port = sqla.Column(sqla.Integer)

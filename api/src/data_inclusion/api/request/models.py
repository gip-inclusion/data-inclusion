from sqlalchemy.orm import Mapped

from data_inclusion.api.core import db


class Request(db.Base):
    id: Mapped[db.uuid_pk]
    created_at: Mapped[db.timestamp]
    status_code: Mapped[int]
    method: Mapped[str]
    path: Mapped[str]
    base_url: Mapped[str]
    user: Mapped[str | None]
    path_params: Mapped[dict]
    query_params: Mapped[dict]
    client_host: Mapped[str | None]
    client_port: Mapped[int | None]
    endpoint_name: Mapped[str | None]

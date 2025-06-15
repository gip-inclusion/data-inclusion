import fastapi

from data_inclusion.api.inclusion_data.v0 import models as models_v0
from data_inclusion.api.inclusion_data.v1 import models as models_v1


def get_service_model(
    request: fastapi.Request,
) -> type[models_v0.Service] | type[models_v1.Service] | None:
    if "/v0/" in request.url.path:
        return models_v0.Service
    elif "/v1/" in request.url.path:
        return models_v1.Service

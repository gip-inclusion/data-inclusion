import logging

import fastapi

from data_inclusion.api.core.request.services import save_request

logger = logging.getLogger(__name__)


async def save_request_middleware(request: fastapi.Request, call_next):
    response = fastapi.Response("Internal server error", status_code=500)
    try:
        response = await call_next(request)
    except:
        raise
    finally:
        try:
            save_request(request, response, db_session=request.state.db_session)
        except Exception as err:
            logger.error(err)
            pass
    return response

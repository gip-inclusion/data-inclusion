import ua_parser

import fastapi


def is_bot(request_or_user_agent: fastapi.Request | str) -> bool:
    user_agent = (
        request_or_user_agent.headers.get("User-Agent")
        if isinstance(request_or_user_agent, fastapi.Request)
        else request_or_user_agent
    )

    if not user_agent:
        return False  # probably most of our consumers :/
    match ua_parser.parse(user_agent):
        case ua_parser.Result(device=ua_parser.Device(family="Spider")):
            return True
        case _:
            return False

def format_name(nom: str | None, max_length: int = 150) -> str | None:
    if not nom:
        return None
    if len(nom) > max_length:
        return nom[: max_length - 1] + "..."
    return nom


def format_description(desc: str | None, max_length: int = 10000) -> str | None:
    if not desc:
        return None
    if len(desc) > max_length:
        return desc[: max_length - 1] + "..."
    return desc

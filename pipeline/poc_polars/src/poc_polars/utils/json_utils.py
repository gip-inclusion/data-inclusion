import json


def parse_json_array(value) -> list | None:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        if value.startswith("{") and value.endswith("}"):
            inner = value[1:-1]
            if not inner:
                return []
            elements = []
            current = ""
            in_quotes = False
            for char in inner:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == "," and not in_quotes:
                    elements.append(current.strip('"'))
                    current = ""
                else:
                    current += char
            if current:
                elements.append(current.strip('"'))
            return elements
        return [value] if value else None
    return None

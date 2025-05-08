from pydantic import BaseModel


class FrameworkValue(BaseModel):
    """Schema for members of a domain taxonomy"""

    value: str
    label: str
    description: str | None = None

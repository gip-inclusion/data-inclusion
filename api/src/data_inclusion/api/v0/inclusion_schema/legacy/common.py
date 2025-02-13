from typing import Annotated, TypeAlias

from pydantic import StringConstraints

CodePostal: TypeAlias = Annotated[
    str,
    StringConstraints(min_length=5, max_length=5, pattern=r"^\d{5}$"),
]

CodeRna: TypeAlias = Annotated[
    str,
    StringConstraints(min_length=10, max_length=10, pattern=r"^W\d{9}$"),
]

CodeSiren: TypeAlias = Annotated[
    str,
    StringConstraints(min_length=9, max_length=9, pattern=r"^\d{9}$"),
]

CodeSiret: TypeAlias = Annotated[
    str,
    StringConstraints(min_length=14, max_length=14, pattern=r"^\d{14}$"),
]

CodeCommune: TypeAlias = Annotated[
    str,
    StringConstraints(min_length=5, max_length=5, pattern=r"^\w{5}$"),
]

CodeEPCI: TypeAlias = CodeSiren

CodeDepartement: TypeAlias = Annotated[
    str,
    StringConstraints(min_length=2, max_length=3, pattern=r"^\w{2,3}$"),
]

CodeRegion: TypeAlias = Annotated[
    str,
    StringConstraints(min_length=2, max_length=2, pattern=r"^\d{2}$"),
]

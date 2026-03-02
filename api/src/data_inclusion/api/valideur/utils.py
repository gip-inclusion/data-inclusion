from itertools import groupby


def display_ranges(items: list[int]) -> str:
    ranges = [
        [item for _, item in group]
        for _, group in groupby(enumerate(sorted(items)), lambda t: t[0] - t[1])
    ]

    def format_range(r: list[int]) -> str:
        if len(r) == 1:
            return str(r[0])
        else:
            return f"{r[0]}-{r[-1]}"

    return ", ".join(format_range(r) for r in ranges)

import markdownify


def html_to_markdown(content: str | None) -> str | None:
    """Convert HTML content to Markdown format."""

    if content is None or content == "":
        return content

    return markdownify.markdownify(
        html=content,
        bullets="*",
        heading_style="ATX",  # use # for headings
        strip=["img"],  # ignore images
    )

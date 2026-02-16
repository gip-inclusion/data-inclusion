import html_to_markdown
import pytest


@pytest.mark.parametrize(
    ("content", "expected_markdown"),
    [
        (None, None),
        ("", ""),
        ("just plain text", "just plain text"),
        ("<h1>Hello World</h1>", "# Hello World"),
        ("<ul><li>Item 1</li><li>Item 2</li></ul>", "* Item 1\n* Item 2"),
        ("<a href='https://example.com'>Link</a>", "[Link](https://example.com)"),
        (
            "<a href='https://example.com'>https://example.com</a>",
            "[https://example.com](https://example.com)",
        ),
        ('<img src="data:image/png;base64,XXX=">', ""),
    ],
)
def test_html_to_markdown(content, expected_markdown):
    assert html_to_markdown.html_to_markdown(content=content) == expected_markdown

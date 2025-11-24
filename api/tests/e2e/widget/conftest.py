import pytest


@pytest.fixture(scope="session")
def base_url():
    return "http://localhost:8000"


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args, base_url):
    return {
        **browser_context_args,
        "base_url": base_url,
    }

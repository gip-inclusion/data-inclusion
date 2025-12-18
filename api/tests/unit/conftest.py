import pytest


@pytest.fixture
def auth_disabled(monkeypatch):
    from data_inclusion.api.widget.routes import settings as widget_settings

    monkeypatch.setattr(widget_settings, "TOKEN_ENABLED", False)

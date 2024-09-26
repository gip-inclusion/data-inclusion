def pytest_addoption(parser):
    parser.addoption("--update-duplicates", action="store_true", default=False)

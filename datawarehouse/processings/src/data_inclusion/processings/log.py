import logging
import sys


def setup_logging(name):
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            # hardcode UTC as the timezone since resolving it would be too complex
            # we want to be as close as the default postgresql logging as possible
            "%(asctime)s.%(msecs)03d UTC [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

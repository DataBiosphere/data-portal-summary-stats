import logging

from dpss.config import config

MEMORY_USAGE = logging.INFO - 1


def setup_log(log_name: str, *handlers: logging.Handler) -> logging.Logger:
    log = logging.getLogger(log_name)
    log.setLevel(config.log_level)
    log.propagate = False
    formatter = logging.Formatter('%(asctime)s %(message)s')
    if not handlers:
        handlers = [logging.StreamHandler()]
    for handler in handlers:
        handler.setLevel(config.log_level)
        handler.setFormatter(formatter)
        log.addHandler(handler)
    return log

from pathlib import Path
from logging import config, getLogger


def get_logger():
    """Return a Logger instance appropriate for using in a Tap or a Target."""
    path = Path(__file__).parent / 'logging.conf'
    print(path)
    # See
    # https://docs.python.org/3.5/library/logging.config.html#logging.config.fileConfig
    # for a discussion of why or why not to set disable_existing_loggers
    # to False. The long and short of it is that if you don't set it to
    # False it ruins external module's abilities to use the logging
    # facility.
    config.fileConfig(path, disable_existing_loggers=False)
    return getLogger()


def log_debug(msg, *args, **kwargs):
    get_logger().debug(msg, *args, **kwargs)


def log_info(msg, *args, **kwargs):
    get_logger().info(msg, *args, **kwargs)


def log_warning(msg, *args, **kwargs):
    get_logger().warning(msg, *args, **kwargs)


def log_error(msg, *args, **kwargs):
    get_logger().error(msg, *args, **kwargs)


def log_critical(msg, *args, **kwargs):
    get_logger().critical(msg, *args, **kwargs)


def log_fatal(msg, *args, **kwargs):
    get_logger().fatal(msg, *args, **kwargs)


def log_exception(msg, *args, **kwargs):
    get_logger().exception(msg, *args, **kwargs)

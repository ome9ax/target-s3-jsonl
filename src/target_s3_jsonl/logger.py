from pathlib import Path
from logging import config, getLogger


def get_logger():
    '''Return a Logger instance appropriate for using in a Tap or a Target.'''
    # See
    # https://docs.python.org/3.5/library/logging.config.html#logging.config.fileConfig
    # for a discussion of why or why not to set disable_existing_loggers
    # to False. The long and short of it is that if you don't set it to
    # False it ruins external module's abilities to use the logging
    # facility.
    config.fileConfig(Path(__file__).parent / 'logging.conf', disable_existing_loggers=False)
    return getLogger()

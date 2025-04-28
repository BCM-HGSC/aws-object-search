from logging import (
    basicConfig,
    ERROR,
    getLevelNamesMapping,
    getLogger,
    INFO,
    StreamHandler,
)

try:
    from rich.logging import RichHandler
except:
    ROOT_HANDLER = StreamHandler()
    ROOT_FORMAT = "%(levelname)s %(asctime)s %(name)s: %(message)s"
else:
    ROOT_HANDLER = RichHandler(show_time=False)
    ROOT_FORMAT = "%(asctime)s %(name)s: %(message)s"


def config_logging(log_level: int | str | None) -> None:
    """
    Configure the logging settings for the application.
    """
    if log_level is None:
        level = INFO
    elif isinstance(log_level, str):
        level = getLevelNamesMapping().get(log_level.upper())
    elif isinstance(log_level, int):
        level = log_level
    else:
        raise TypeError("log_level must be int, str or None")
    getLogger("botocore.credentials").setLevel(ERROR)
    getLogger("botocore.tokens").setLevel(ERROR)
    basicConfig(level=level, format=ROOT_FORMAT, handlers=[ROOT_HANDLER])

import logging
import sys

logger = logging.getLogger("__name__")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(module)s.%(funcName)s: %(message)s", "%Y-%m-%d %H:%M:%S"
))
logger.addHandler(handler)

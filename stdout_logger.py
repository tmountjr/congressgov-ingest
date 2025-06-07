"""stdout logging class with a minimum errorlevel logging."""

import sys
import logging


class StdoutLogger(logging.Logger):
    """stdout Logger."""

    def __init__(self, name: str, min_level=logging.DEBUG):
        super().__init__(name, min_level)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(min_level)

        self.addHandler(handler)

# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseOracleCapture"
]

import os.path

import settings
import core.capture


class BaseOracleCapture(core.capture.BaseCaptureItem):
    """oracle数据采集"""

    RELATIVE_IMPORT_TOP_PATH_PREFIX = settings.SETTINGS_FILE_DIR

    PATH_TO_IMPORT = lambda: os.path.dirname(os.path.dirname(__file__))


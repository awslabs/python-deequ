# -*- coding: utf-8 -*-
from importlib.metadata import version

import pydeequ


def test_package_version_is_exposed():
    assert pydeequ.__version__ == version("pydeequ")

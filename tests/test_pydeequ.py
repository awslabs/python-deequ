# -*- coding: utf-8 -*-
from pydeequ import __version__


def test_version():
    if __version__ != "1.0.0":
        raise AssertionError

# -*- coding: utf-8 -*-
from pydeequ import __version__


def test_version():
    if __version__ != "0.1.5":
        raise AssertionError

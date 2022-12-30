import pytest
from pydeequ.configs import _extract_major_minor_versions


@pytest.parametrize(
    "full_version, major_minor_version",
    [
        ("3.2.1", "3.2"),
        ("3.1", "3.1"),
        ("3.10.3", "3.10"),
        ("3.10", "3.10")
    ]
)
def test_extract_major_minor_versions(full_version, major_minor_version):
    assert _extract_major_minor_versions(full_version) == major_minor_version

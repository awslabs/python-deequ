from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

def setup_package():
    setup(
        name='pydeequ',
        version="0.1.7",
        author="PyDeequ Developers",
        author_email="calviwan@amazon.com",
        description="Python API for Deequ",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/awslabs/python-deequ",
        packages=['pydeequ'],
        package_dir={
            "pydeequ": "pydeequ"
        },
        classifiers=[
            "Development Status :: 4 - Beta",
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: Apache Software License"
        ],
        install_requires=['pandas'],
        setup_requires=['pyspark==2.4.7', 'pytest-runner', 'pandas'],
        tests_require=['pyspark==2.4.7', 'pytest', 'pandas']

    )

if __name__ == "__main__":
    setup_package()

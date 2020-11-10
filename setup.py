from setuptools import setup, find_packages

VERSION="0.1.3"
with open("README.md", "r") as fh:
    long_description = fh.read()

def setup_package():
    setup(
        name='pydeequ',
        version="0.1.2",
        author="Christopher Ghyzel",
        author_email="cghyzel@amazon.com",
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
        install_requires=['pyspark>=2.4.5'],
        setup_requires=['pyspark>=2.4.5', 'pytest-runner'],
        tests_require=['pyspark>=2.4.5','pytest']

    )

if __name__ == "__main__":
    setup_package()

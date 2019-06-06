import pathlib
import re

import setuptools


__version__ = re.search(
    r"__version__ = ['\"]([^'\"]*)['\"]",
    pathlib.Path("mds", "versions.py").read_text()
    ).group(1)

setuptools.setup(
    name="mds-provider",
    version=__version__,
    description="Tools for working with Mobility Data Specification Provider data",
    long_description=pathlib.Path("README.md").read_text(),
    url="https://github.com/CityofSantaMonica/mds-provider",
    author="City of Santa Monica and contributors",
    license="MIT",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        "Fiona",
        "jsonschema",
        "packaging",
        "pandas",
        "psycopg2-binary",
        "python-dateutil",
        "requests",
        "scipy",
        "Shapely",
        "sqlalchemy"
    ],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Natural Language :: English",
    ],
)
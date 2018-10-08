import os
import re
from setuptools import find_packages, setup


READMEFILE = "README.md"
VERSIONFILE = os.path.join("mds", "_version.py")
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"


def get_version():
    verstrline = open(VERSIONFILE, "rt").read()
    mo = re.search(VSRE, verstrline, re.M)
    if mo:
        return mo.group(1)
    else:
        raise RuntimeError("Unable to find version string in %s." % VERSIONFILE)


setup(
    name="mds_provider",
    version=get_version(),
    description="Tools for working with Mobility Data Specification Provider data",
    long_description=open(READMEFILE).read(),
    url="https://github.com/CityofSantaMonica/mds-provider",
    author="City of Santa Monica and contributors",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Fiona",
        "geopandas",
        "jsonschema",
        "numpy",
        "pandas",
        "psycopg2-binary",
        "requests",
        "scipy",
        "Shapely",
        "sqlalchemy"
    ],
    classifiers=[
        "Environment :: Docker",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Natural Language :: English",
    ],
)
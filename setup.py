#!/usr/bin/env python
from setuptools import setup, find_packages
import os
import io

here = os.path.abspath(os.path.dirname(__file__))

# get the dependencies and installs
with io.open(os.path.join(here, "requirements.txt"), encoding="utf-8") as f:
    all_reqs = f.read().split("\n")

install_requires = [x.strip() for x in all_reqs if "git+" not in x]
dependency_links = [x.strip().replace("git+", "") for x in all_reqs if "git+" not in x]
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="landsat2geojson",
    author="Junior Flores",
    author_email="junior@developmentseed.org",
    version="0.0.2",
    description="Script to extract features from landsat",
    long_description=long_description,
    url="https://github.com/yunica/landsat2geojson",
    keywords="",
    entry_points={"console_scripts": ["landsat2geojson = landsat2geojson.main:main", ]},
    packages=find_packages(exclude=["docs", "tests*"]),
    include_package_data=True,
    install_requires=install_requires,
    dependency_links=dependency_links,
)

#!/usr/bin/env python
import io
import os

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))

# get the dependencies and installs
with io.open(os.path.join(here, "requirements.txt"), encoding="utf-8") as f:
    all_reqs = f.read().split("\n")


def get_name_repo(url):
    """get the name of package from git repository url"""
    return url.split("/")[-1].split(".")[0]


install_requires = [x.strip() for x in all_reqs if "git+" not in x]
install_requires += [f"{get_name_repo(x)} @ {x}" for x in all_reqs if "git+" in x]
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="landsat2geojson",
    author="Junior Flores",
    author_email="junior@developmentseed.org",
    version="0.0.3",
    description="Script to extract features from landsat",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/yunica/landsat2geojson",
    keywords=[
        "earth observation",
        "remote sesing",
        "GIS",
        "satellite imagery",
        "landsat",
        "water",
        "usgs",
        "geojson",
        "overpass"
    ],
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Science/Research",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development",
    ],
    entry_points={
        "console_scripts": [
            "landsat2geojson = landsat2geojson.main:main",
        ]
    },
    packages=find_packages(exclude=["docs", "tests*"]),
    include_package_data=True,
    install_requires=install_requires,
    python_requires='>=3.9'
)

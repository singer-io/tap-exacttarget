#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="tap-exacttarget",
    version="2.0.0",
    description="Singer.io tap for extracting data from the ExactTarget API",
    author="Singer.io",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    packages=find_packages(),
    install_requires=[
        "singer-python==6.1.1",
        "zeep==4.3.1",
        "requests==2.32.4",
    ],
    extras_require={
        "dev": ["pylint==4.0.0", "nose2==0.15.1"],
    },
    entry_points={
        "console_scripts": [
            "tap-exacttarget=tap_exacttarget:main"
        ]
    },
    package_data={"tap_exacttarget": ["schemas/*.json"]},
)

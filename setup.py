#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="tap-exacttarget",
    version="2.0.0",
    description="Singer.io tap for extracting data from the ExactTarget API",
    author="Fishtown Analytics",
    url="http://fishtownanalytics.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_exacttarget"],
    install_requires=[
        "singer-python==5.13.0",
        "zeep==4.3.1",
        "requests==2.32.4",
    ],
    extras_require={
        "test": ["pylint==2.10.2", "astroid==2.7.3", "nose"],
    },
    entry_points="""
    [console_scripts]
    tap-exacttarget=tap_exacttarget:main
    """,
    packages=find_packages(),
    package_data={"tap_exacttarget": ["schemas/*.json"]},
)

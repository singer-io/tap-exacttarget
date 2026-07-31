#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="tap-exacttarget",
    version="2.3.1",
    description="Singer.io tap for extracting data from the ExactTarget API",
    author="Singer.io",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    packages=find_packages(),
    install_requires=[
        "singer-python==6.8.0",
        "zeep==4.3.3",
        "requests==2.34.2",
    ],
    extras_require={
        'dev': [
            'pylint',
            'nose2',
            'parameterized',
            'pytest',
            'pytest-cov',
            'coverage',
        ]
      },
    entry_points={
        "console_scripts": [
            "tap-exacttarget=tap_exacttarget:main"
        ]
    },
    package_data={"tap_exacttarget": ["schemas/*.json"]},
)

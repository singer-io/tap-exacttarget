#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-exacttarget',
    version='1.6.1',
    description='Singer.io tap for extracting data from the ExactTarget API',
    author='Fishtown Analytics',
    url='http://fishtownanalytics.com',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_exacttarget'],
    install_requires=[
        'funcy==1.9.1',
        'singer-python>=5.9.0',
        'python-dateutil>=2.6.0',
        'voluptuous==0.10.5',
        'Salesforce-FuelSDK==1.3.0'
    ],
    extras_require={
        'dev': [
            'ipdb==0.11',
            'pylint==2.1.1',
            'astroid==2.1.0',
            'nose'
        ]
    },
    entry_points='''
    [console_scripts]
    tap-exacttarget=tap_exacttarget:main
    ''',
    packages=find_packages()
)

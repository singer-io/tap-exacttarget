#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-exacttarget',
    version='1.0.5',
    description='Singer.io tap for extracting data from the ExactTarget API',
    author='Fishtown Analytics',
    url='http://fishtownanalytics.com',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_exacttarget'],
    install_requires=[
        'funcy==1.9.1',
        'singer-python>=5.0.6',
        'python-dateutil==2.6.0',
        'voluptuous==0.10.5',
        'pyjwt>=0.1.9',
        'requests>=2.2.1',
        'suds-jurko>=0.6'
    ],
    entry_points='''
    [console_scripts]
    tap-exacttarget=tap_exacttarget:main
    ''',
    packages=find_packages()
)

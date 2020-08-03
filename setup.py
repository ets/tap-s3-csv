#!/usr/bin/env python

from setuptools import setup

setup(name='tap-s3-csv',
      version='0.0.1',
      description='Singer.io tap for extracting CSV files from S3',
      author='Eric Simmerman',
      url='https://github.com/ets/tap-s3-csv',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_s3_csv'],
      install_requires=[
          'boto3>=1.14.33', 
          'smart_open>=2.1',
          'singer-python>=5.0',
          'voluptuous>=0.10.5',
          'xlrd',
      ],
      entry_points='''
          [console_scripts]
          tap-s3-csv=tap_s3_csv:main
      ''',
      packages=['tap_s3_csv'])

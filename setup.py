#!/usr/bin/env python
# -*- coding: utf8 -*-

from setuptools import setup, find_packages

setup(
    name='mnrdmq',
    python_requires='>=3.7.0',
    version='0.1.5',
    license='BSD',
    packages=find_packages(),
    #include_package_data=True,
    long_description="Simple controller-agent framework",
    install_requires=open("requirements.txt").read()
)

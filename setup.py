# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from src import __version__

requires = [
    'requests==2.20.1',
    'pyserial==3.4',
    'urllib3'
]

setup(
    name='GravityCollector-Client',
    version=__version__,
    packages=find_packages(),
    package_data={},
    python_requires='>=3.6',
    include_package_data=True,
    install_requires=requires,
    author='Zachery Brady',
    author_email='bradyzp@dynamicgravitysystems.com',
    url='https://github.com/bradyzp/GravityCollector-Client',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3.6',
        'Topic :: Internet :: WWW/HTTP :: WSGI :: Application'
    ]
)

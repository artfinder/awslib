# Use setuptools if we can
try:
    from setuptools.core import setup
except ImportError:
    from distutils.core import setup

PACKAGE = 'awslib'
VERSION = '0.6.7'

setup(
    name=PACKAGE, version=VERSION,
    description="Small library providing Amazon Web Services utility functions on top of boto",
    packages=['awslib'],
    license='MIT',
    author='Art Discovery Ltd',
    maintainer='Art Discovery Ltd',
    maintainer_email='tech@artfinder.com',
    install_requires=[
        'boto',
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
    ],
)

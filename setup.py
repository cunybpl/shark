from setuptools import setup, find_packages
import os

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [
    'pandas>=1.1',]

test_requirements = [
    'pytest',
    'pytest-cov', 
    'pytest-mock'
]

NAME = 'shark'

# build the version from _version.py
here = os.path.abspath(os.path.dirname(__file__))
about = {}
project_slug = NAME.lower().replace("-", "_").replace(" ", "_")
with open(os.path.join(here, project_slug, '_version.py')) as f:
    exec(f.read(), about)


setup(
    name=NAME,
    description= 'dataframe extension package for cleaning and resampling time series data',
    version=about['VERSION'],
    long_description=readme,
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests", "protos", "scripts"]),
    include_package_data=True,
    install_requires=requirements,
    test_suite='pytest',
    tests_require=test_requirements,
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython'
    ],
)

from setuptools import setup, find_packages

from data_census.util.env import PROJECT_NAME


def readme():
    try:
        with open('README.md') as f:
            return f.read()
    except IOError:
        return ''


dependencies: list = [
    'simpledbf',
    'geopandas',
]

setup(
    name=PROJECT_NAME,
    version='0.0.1',
    # url=
    long_description=readme(),
    description='Access and download Census data.',
    author='Daniel M. Sullivan',
    author_email='sullydm@gmail.com',
    packages=find_packages(),
    tests_require=[
        'pytest',
    ],
    package_data={PROJECT_NAME.replace('-', '_'): ["py.typed"]},
    # include_package_data=True,        # To copy stuff in `MANIFEST.in`
    # install_requires=dependencies,
    zip_safe=False,
    license='BSD'
)

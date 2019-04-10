from setuptools import setup, find_packages


def readme():
    try:
        with open('README.md') as f:
            return f.read()
    except:
        return ''


dependencies = [
]


setup(
    name='census-data',
    version='0.0.1',
    description='',
    long_description=readme(),
    author='Daniel M. Sullivan',
    packages=find_packages(),
    tests_require=[
        'pytest',
    ],
    package_data={'census_data': ["py.typed"]},
    install_requires=dependencies,
    zip_safe=False,
    license='BSD'
)

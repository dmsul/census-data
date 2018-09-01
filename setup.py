from setuptools import setup, find_packages


def readme():
    try:
        with open('README.md') as f:
            return f.read()
    except:
        return ''


# dependencies = []
    # 'numpy>=1.9.2',
    # 'pandas>=0.16.0',

setup(name='census-data',
      version='0.0.1',
      description='',
      # long_description=readme(),
      # url=
      author='Daniel M. Sullivan',
      # author_email=
      packages=find_packages(),
      # install_requires=dependencies,
      # tests_require=[ 'nose', ],
      # include_package_data=True,        # To copy stuff in `MANIFEST.in`
      # dependency_links=['http://
      # zip_safe=False
      # license='BSD'
      )

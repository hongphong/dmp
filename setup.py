from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow_dmp_provider setup."""
setup(
    name='airflow_dmp_provider',
    version="0.0.1",
    description='DMP provider.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        # "apache_airflow_provider": [
        #
        # ]
    },
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['pandas', 'numpy', 'matplotlib', 'pyjanitor', 'dataframe_sql', 'seaborn', 'plotly',
                      'pivottablejs'],
    setup_requires=['setuptools', 'wheel'],
    author='phonph16',
    author_email='phongph16@viettel.com.vn',
    url='phongph16@viettel.com.vn',
    python_requires='~=3.6',
)

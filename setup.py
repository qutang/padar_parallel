from setuptools import setup, find_packages

setup(
    name='padar_parallel',
    version='0.1.1',
    packages=find_packages(),
    include_package_data=True,
    description='''Extension to do parallel computing on grouped chunks or files
    for padar package''',
    long_description=open('README.md').read(),
    install_requires=[
        "dask[complete]",
        "padar_converter"
    ],
)

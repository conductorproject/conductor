import os
from setuptools import setup, find_packages

execfile('conductor/version.py')
README = open(os.path.join(os.path.dirname(__file__), "README.rst")).read()

setup(
    name='conductor',
    version=__version__,
    description='A generic processing framework',
    long_description=README,
    author='Ricardo Silva',
    author_email='ricardo.silva@ipma.pt',
    url='',
    classifiers=[''],
    platforms=[''],
    license='',
    packages=find_packages(exclude=["*.tests", "*.tests.*",
                                    "tests.*", "tests"]),
    entry_points={
        'console_scripts': [
            'install_giosystem_hdf5 = giosystemcore.scripts.'
            'installhdf5:main',
            'install_giosystem_algorithms = giosystemcore.'
            'scripts.installalgorithms:main',
        ],
    },
    include_package_data=True,
    install_requires=[
        'ftputil',
        'enum34',  # python 3.4 enum class backported to earlier versions
    ]
)

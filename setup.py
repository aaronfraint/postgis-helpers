from setuptools import find_packages, setup

setup(
    name='postgis_helpers',
    packages=find_packages(),
    version='0.2.2',
    description='Python helpers that facilitate SQL data I/O',
    author='Aaron Fraint, AICP',
    license='MIT',
    entry_points="""
        [console_scripts]
        pGIS=postgis_helpers.cli:main
    """,
)

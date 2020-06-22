from setuptools import find_packages, setup

setup(
    name='postgis_helpers',
    packages=find_packages(),
    version='0.1.0',
    description='Python helpers that facilitate SQL data I/O',
    author='Aaron Fraint, AICP',
    license='MIT',
    entry_points="""
        [console_scripts]
        export_all_dbs_on_host=postgis_helpers.procedures.export_all_dbs_on_host:back_up_entire_cluster
    """,
)

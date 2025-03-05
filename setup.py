from setuptools import find_packages, setup

setup(
    name="etl_wo",
    packages=find_packages(exclude=["etl_wo_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

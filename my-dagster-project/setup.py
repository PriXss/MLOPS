from setuptools import find_packages, setup

setup(
    name="my_dagster_project",
    packages=find_packages(exclude=["my_dagster_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "boto3",
        "dvc",
        "subprocess",
        

    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

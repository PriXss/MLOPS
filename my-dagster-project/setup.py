from setuptools import find_packages, setup

setup(
    name="quickstart_etl",
    packages=find_packages(exclude=["quickstart_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "dvc",
        "dvc-s3",
        "alpaca_trade_api"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

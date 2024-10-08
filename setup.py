from setuptools import find_packages, setup

setup(
    name="cl_wrappers_aws",
    packages=find_packages(include=["cl_wrappers_aws", "cl_wrappers_aws.*"]),
    install_requires=[
        "dagster",
        "dagster-aws",
        "dagster-cloud",
        "pandas",
        "matplotlib",
        "requests"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)

"""stac_fastapi: pgstac module."""

from setuptools import find_namespace_packages, setup

with open("README.md") as f:
    desc = f.read()

install_requires = [
    "attrs",
    "orjson",
    "pydantic",
    "stac-fastapi.api>=6.0,<7.0",
    "stac-fastapi.extensions>=6.0,<7.0",
    "stac-fastapi.types>=6.0,<7.0",
    "asyncpg",
    "buildpg",
    "brotli_asgi",
    "cql2>=0.3.6",
    "pypgstac>=0.8,<0.10",
    "typing_extensions>=4.9.0",
]

extra_reqs = {
    "dev": [
        "pystac[validation]",
        "pypgstac[psycopg]==0.9.*",
        "pytest-postgresql",
        "pytest",
        "pytest-cov",
        "pytest-asyncio>=0.17,<1.1",
        "pre-commit",
        "requests",
        "shapely",
        "httpx",
        "twine",
        "wheel",
    ],
    "docs": [
        "black>=23.10.1",
        "mkdocs>=1.4.3",
        "mkdocs-jupyter>=0.24.5",
        "mkdocs-material[imaging]>=9.5",
        "griffe-inherited-docstrings>=1.0.0",
        "mkdocstrings[python]>=0.25.1",
    ],
    "server": ["uvicorn[standard]==0.34.3"],
    "awslambda": ["mangum"],
}


setup(
    name="stac-fastapi.pgstac",
    description="An implementation of STAC API based on the FastAPI framework and using the pgstac backend.",
    long_description=desc,
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
    ],
    keywords="STAC FastAPI COG",
    author="David Bitner",
    author_email="david@developmentseed.org",
    url="https://github.com/stac-utils/stac-fastapi",
    license="MIT",
    packages=find_namespace_packages(exclude=["tests", "scripts"]),
    zip_safe=False,
    install_requires=install_requires,
    tests_require=extra_reqs["dev"],
    extras_require=extra_reqs,
    entry_points={"console_scripts": ["stac-fastapi-pgstac=stac_fastapi.pgstac.app:run"]},
)

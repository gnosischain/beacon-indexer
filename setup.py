from setuptools import setup, find_packages

setup(
    name="beacon-chain-scraper",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiohttp>=3.8.5",
        "asyncio>=3.4.3",
        "clickhouse-driver>=0.2.6",
        "pydantic>=2.3.0",
        "python-dotenv>=1.0.0",
        "tenacity>=8.2.3",
        "pyyaml>=6.0.1",
    ],
    entry_points={
        "console_scripts": [
            "beacon-scraper=src.main:main",
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="A beacon chain scraper for Ethereum",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/beacon-chain-scraper",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
)
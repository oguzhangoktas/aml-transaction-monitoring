"""
Setup script for AML Transaction Monitoring System
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="aml-transaction-monitoring",
    version="1.0.0",
    author="Oğuzhan Göktaş",
    author_email="oguzhangoktas22@gmail.com",
    description="Real-time AML transaction monitoring system with Spark Streaming and Delta Lake",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oguzhangoktas/aml-transaction-monitoring",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-cov>=4.1.0",
            "black>=23.12.1",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "aml-generate-data=data_generator.transaction_producer:main",
        ],
    },
)

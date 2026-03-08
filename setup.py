from setuptools import setup, find_packages

setup(
    name="sdgis-cli",
    version="1.0.5",
    description="CLI for the San Diego Regional Data Warehouse (SANDAG/SanGIS)",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Your Name",
    license="MIT",
    py_modules=["sdgis"],
    install_requires=[
        "click>=8.0",
        "requests>=2.28",
        "rich>=13.0",
    ],
    extras_require={
        "embed": [
            "sentence-transformers>=2.0",
            "numpy>=1.20",
        ],
        "map": [
            "staticmap>=0.5",
        ],
        "all": [
            "sentence-transformers>=2.0",
            "numpy>=1.20",
            "staticmap>=0.5",
        ],
    },
    entry_points={
        "console_scripts": [
            "sdgis=sdgis:cli",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: GIS",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
)

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="flywheel_bids_tools",
    version="0.0.1",
    author="Tinashe M. Tapera",
    author_email="tinashemtapera@gmail.com",
    description="Tools for extracting, editing, and uploading BIDS information from and to Flywheel",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/PennBBL/bids-on-flywheel",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)

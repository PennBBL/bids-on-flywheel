import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="flywheel_bids_tools",
    version="0.0.7",
    author="Tinashe M. Tapera",
    author_email="tinashemtapera@gmail.com",
    description="Tools for extracting, editing, and uploading BIDS data from and to Flywheel",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/PennBBL/bids-on-flywheel",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points = {
        'console_scripts': [
            'query-bids=flywheel_bids_tools.query_bids:main',
            'upload-bids=flywheel_bids_tools.upload_bids:main',
            'group-query=flywheel_bids_tools.group_query:main',
            'ungroup-query=flywheel_bids_tools.ungroup_query:main',
            'query-gears=flywheel_bids_tools.query_gears:main',
            'run-gear=flywheel_bids_tools.run_gear:main'
        ],
    }
)

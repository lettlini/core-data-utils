from setuptools import find_packages, setup

setup(
    name="core-data-utils",
    version="1",
    author="Kolya Lettl",
    author_email="kolya.lettl@studserv.uni-leipzig.de",
    description="framework for working with data structures I frequently use when building data pipelines for research",
    packages=find_packages(exclude="tests"),
    install_requires=["tqdm"],
    extras_require={"image": ["opencv-python"], "graph": ["networkx"]},
)

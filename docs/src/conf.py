"""Sphinx documentation generation configuration."""

import importlib.metadata

project = "swf-typed"
copyright = "2021, Laurie O"
author = "Laurie O"
release = importlib.metadata.version("swf-typed")  # full version
version = ".".join(release.split(".")[:2])  # short X.Y version

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "autodocsumm",
]

html_theme = "sphinx_rtd_theme"

"""Sphinx configuration for get-weather-data documentation."""

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "Get Weather Data"
copyright = "2024, Suriyan Laohaprapanon, Gaurav Sood"
author = "Suriyan Laohaprapanon, Gaurav Sood"
version = "3.0.0"
release = "3.0.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = ["_static"]
html_title = "Get Weather Data"

myst_enable_extensions = [
    "colon_fence",
]

autodoc_member_order = "bysource"
autodoc_typehints = "description"

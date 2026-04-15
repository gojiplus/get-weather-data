# -*- coding: utf-8 -*-
#
# get-weather-data documentation build configuration file

import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

project = "Get Weather Data"
copyright = "2016-2024, Suriyan Laohaprapanon, Gaurav Sood"
author = "Suriyan Laohaprapanon, Gaurav Sood"

version = "0.2.0"
release = "0.2.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.todo",
    "sphinx_autodoc_typehints",
]

templates_path = ["_templates"]
source_suffix = ".rst"
master_doc = "index"
language = "en"
exclude_patterns = []
pygments_style = "sphinx"
todo_include_todos = False

html_theme = "furo"
html_static_path = ["_static"]
htmlhelp_basename = "get-weather-datadoc"

latex_elements = {}

latex_documents = [
    (
        master_doc,
        "get-weather-data.tex",
        "Get Weather Data Documentation",
        "Suriyan Laohaprapanon, Gaurav Sood",
        "manual",
    ),
]

man_pages = [
    (master_doc, "get-weather-data", "Get Weather Data Documentation", [author], 1)
]

texinfo_documents = [
    (
        master_doc,
        "get-weather-data",
        "Get Weather Data Documentation",
        author,
        "get-weather-data",
        "Scripts for finding out the weather in a particular zip code.",
        "Miscellaneous",
    ),
]

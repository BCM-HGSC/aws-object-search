#!/usr/bin/env python3

"""
This script is a wrapper for the aos-search command line tool.
It specifies the location of the installed tool and the location of the catalog
and then executes the command with the user-provided arguments.
"""

import os
import pathlib
import sys


# Assume that everything is up one level from the current file.
HERE = pathlib.Path(__file__).parent.resolve()
ROOT = HERE.parent
PATH_TO_AOS_SEARCH = ROOT / "env/bin/aos-search"
PATH_TO_CATALOG = ROOT / "s3_objects"

os.execv(PATH_TO_AOS_SEARCH, [PATH_TO_AOS_SEARCH, PATH_TO_CATALOG] + sys.argv[1:])

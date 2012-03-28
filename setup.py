#!/usr/bin/env python

from distutils.core import setup
from build_manpage import build_manpage, install_manpage

import sys
sys.path.append('./pgloader/')
from options import PGLOADER_VERSION



setup(name='pgloader',
        version=PGLOADER_VERSION,
        description='PostgreSQL data import tool, see included man page.',
        author='Dimitri Fontaine',
        author_email='<dim@tapoueh.org>',
        url='https://github.com/dimitri/pgloader',
        packages=['pgloader','reformat'],
        scripts=['scripts/pgloader'],
        cmdclass={'build_manpage': build_manpage,'install_manpage':install_manpage}
     )

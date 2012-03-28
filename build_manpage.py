import datetime
import optparse
import subprocess
import shutil
import os
from distutils.command.build import build
from distutils.command.install import install
from distutils.core import Command
from distutils.errors import DistutilsOptionError


class build_manpage(Command):
    """Create the manpage using asciidoc and xmlto utilities"""

    description = 'Generate man page.'

    user_options = [
    ]

    def initialize_options(self):
        self.textfile=self.distribution.get_name()+'.1.txt'

    def finalize_options(self):
        self.xmlfile=self.textfile.replace('.txt','.xml')
        self.announce('Writing manpage')

    def run(self):
        proc=subprocess.Popen(['asciidoc','-d','manpage','-b','docbook',self.textfile],stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE)
        proc.wait()
        proc=subprocess.Popen(['xmlto','man',self.xmlfile],stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE)
        proc.wait()

class install_manpage(Command):
    """Install the manpage"""

    description = 'Install man page.'

    user_options = [
    ]

    def initialize_options(self):
        self.manpagedir=None
        self.manfile=self.distribution.get_name()+'.1'

    def finalize_options(self):
        self.manpagedir='/usr/local/share/man/'

    def run(self):
        if self.manpagedir:
            self.copy_file(self.manfile,os.path.join(self.manpagedir,self.manfile))

#build.sub_commands.append(('build_manpage', None))
#install.sub_commands.append(('install_manpage', None))

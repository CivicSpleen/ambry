"""
Install packages using pip

Copyright (c) 2013 Clarinova. This file is licensed under the terms of the
Revised BSD License, included in this distribution as LICENSE.txt
"""

import pip
from pip.commands import InstallCommand


def install(install_dir,egg,url):
    
    initial_args = ['install', '--install-option=--install-purelib={}'.format(install_dir), url]
    cmd_name, options, args, parser = pip.parseopts(initial_args)
           
    command = InstallCommand(parser)
    return command.main(args[1:], options)
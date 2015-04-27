
# Would use a relative import, but there is a problem realted to -m
# http://stackoverflow.com/a/18888854

import argparse
import sys
from ambry.ui import app, configure_application, write_config, config_paths
from ambry.ui import fscache

parser = argparse.ArgumentParser(
    prog='python -mambry.server.documentation',
    description='Run an Ambry documentation server')

parser.add_argument(
    '-I',
    '--install',
    action='store_true',
    help="Install configuration file and exit")
parser.add_argument('-H', '--host', help="Server host.")
parser.add_argument('-p', '--port', help="Server port")
parser.add_argument('-c', '--cache', help="Generated file cache. ")
parser.add_argument(
    '-P',
    '--use-proxy',
    action='store_true',
    help="Setup for using a proxy in front of server, "
         "using werkzeug.contrib.fixers.ProxyFix")
parser.add_argument(
    '-d',
    '--debug',
    action='store_true',
    help="Set debugging mode")
parser.add_argument(
    '-C',
    '--check-config',
    action='store_true',
    help="print cache string and configuration file and exit")


parser.add_argument(
    '-t',
    '--test',
    action='store_true',
    help="Run a test function in development")

args = parser.parse_args()


config = configure_application(vars(args))

if args.use_proxy:
    from werkzeug.contrib.fixers import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app)


import ambry.ui.views

app.run(host=config['host'], port=int(config['port']), debug=config['debug'])

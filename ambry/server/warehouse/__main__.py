
# Would use a relative import, but there is a problem realted to -m
# http://stackoverflow.com/a/18888854
from ambry.server.warehouse import app, configure_application
from flask import current_app

import argparse


parser = argparse.ArgumentParser(prog='python -mambry.server.documentation',
                                 description='Run an Ambry documentation server')

parser.add_argument('-H', '--host', default=None, help="Server host.")
parser.add_argument('-p', '--port', default=None, help="Server port")
parser.add_argument('-c', '--cache', default=None, help="Generated file cache. ")

parser.add_argument('-P', '--use-proxy', default=False, action='store_true',
                    help="Setup for using a proxy in front of server, usingwerkzeug.contrib.fixers.ProxyFix")

parser.add_argument('-d', '--debug', default=False, action='store_true',
                    help="Set debugging mode")

args = parser.parse_args()

config = {}

if args.port:
    config['port'] = args.port

if args.host:
    config['host'] = args.host

if args.cache:
    config['cache'] = args.cache


if args.use_proxy:
    from werkzeug.contrib.fixers import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app)

config = configure_application(config)

app.run(host = config['host'], port = config['port'], debug = args.debug)



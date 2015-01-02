""" Module for the documentation server, including the blueprint for extracts.

Run with gunicorn:

    gunicorn ambry.library.server:app -b 0.0.0.0:80

"""


from ambry.warehouse.server import exracts_blueprint
import ambrydoc.views

import argparse
import sys
from ambrydoc import app, configure_application, write_config, config_paths
from ambrydoc import fscache

app.register_blueprint(exracts_blueprint)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog='python -mambry.server.documentation',
                                     description='Run an Ambry documentation server')

    parser.add_argument('-I', '--install', action='store_true', help="Install configuration file and exit")
    parser.add_argument('-H', '--host', help="Server host.")
    parser.add_argument('-p', '--port', help="Server port")
    parser.add_argument('-c', '--cache', help="Generated file cache. ")
    parser.add_argument('-P', '--use-proxy', action='store_true',
                        help="Setup for using a proxy in front of server, using werkzeug.contrib.fixers.ProxyFix")
    parser.add_argument('-d', '--debug',  action='store_true',  help="Set debugging mode")
    parser.add_argument('-C', '--check-config',  action='store_true',  help="print cache string and configuration file and exit")
    parser.add_argument('-i', '--index',  action='store_true',  help="Update the search index")
    parser.add_argument('-r', '--reindex',  action='store_true',  help="Recreate the search index")

    parser.add_argument('-s', '--search',  help="search for a term in the search index")

    args = parser.parse_args()

    config = configure_application(vars(args))

    if args.index or args.reindex:
        from ambrydoc.search import Search
        from ambrydoc.cache import DocCache

        print 'Updating the search index'

        s = Search(DocCache(fscache()))

        s.index(reset=args.reindex)

        sys.exit(0)

    if args.search:
        from ambrydoc.search import Search
        from ambrydoc.cache import DocCache

        print "Search for ", args.search

        s = Search(DocCache(fscache()))

        for r in s.search(args.search):
            print r

        sys.exit(0)

    if args.install:
        import yaml
        from ambrydoc import app_config

        path = write_config(app_config,config_paths)

        print app_config

        print "Wrote default config to ", path

        sys.exit(0)

    if args.use_proxy:
        from werkzeug.contrib.fixers import ProxyFix
        app.wsgi_app = ProxyFix(app.wsgi_app)


    if args.check_config:

        import yaml

        print "Using cache: ", fscache()

        sys.exit(0)

    app.run(host = config['host'], port = int(config['port']), debug = config['debug'])

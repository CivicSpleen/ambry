""" Module for the documentation server, including the blueprint for extracts.

Run with gunicorn:

    gunicorn ambry.library.server:app -b 0.0.0.0:80

"""


from ambry.warehouse.server import exracts_blueprint
from ambrydoc import app, configure_application
import logging
from logging import FileHandler
import ambrydoc.views as views

app.register_blueprint(exracts_blueprint,url_prefix='/warehouses')


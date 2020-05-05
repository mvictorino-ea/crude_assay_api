"""Aggregates all separate Namespaces into a Flask-RestX API object"""

from flask_restx import Api
from apis.crude_assay import api as crude_assay
# from .namespace2 import api as ns2
# ...
# from .namespaceX import api as nsX


# Define actual API object
api = Api(
    title='Crude Assay db',
    version='1.0',
    description='Crude Assay Database Operations',
    # All API metadatas
)

# Register all namespaces in the actual API
api.add_namespace(crude_assay, path='/api/v1')  # define custom url_prefix, allowing multiple versions
# api.add_namespace(ns2, path='/api/v1')
# ...
# api.add_namespace(nsX, path='/api/v1')

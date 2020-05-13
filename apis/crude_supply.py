from flask_restx import Namespace, Resource
import os
from dotenv import load_dotenv
import shooju
import pandas as pd

# Initialize RestPlus* Extension API as Namespace
api = Namespace('crude_supply', description='Crude Oil Supply data service')

### Swagger Documentation
# Define Endpoint-specific Parser
parser_crude_supply_get = api.parser()

# Define Endpoint Expected Parameters
# TODO: use only country, expecting country_isos
parser_crude_supply_get.add_argument('country', type=str, required=False, help='Algeria OR Algeria,Congo,...')
parser_crude_supply_get.add_argument('country_iso', type=str, required=False, help='DZ OR DZ,CG,...')
parser_crude_supply_get.add_argument('region', type=str, required=False, help='AFR OR AFR,ME,...')
parser_crude_supply_get.add_argument('date_from', type=str, required=False, help='2017-01-01 OR -2y')
parser_crude_supply_get.add_argument('date_to', type=str, required=False, help='2030-01-01 OR +3M')

#TODO: how to retrieve latest, second latest etc (i.e: -1)
# there is a specific function for that -> get_reported_date
# Look the documentation
parser_crude_supply_get.add_argument('report_date', type=str, required=False, help='2018-01-12 OR -1db +2h')


###
class Base:
    # Load .env file
    load_dotenv()

    # Default Response
    response = {}

    # Establish connection with Shooju
    sj = shooju.Connection(server=os.getenv('SHOOJU_SERVER'), user=os.getenv('SHOOJU_USER'),
                           api_key=os.getenv('SHOOJU_KEY'))

    # Define standard fields
    fields = ['country', 'country_iso', 'region', 'energy_product', 'unit', 'description']

    # Base SID for Crude Oil Supply - OPEC
    base_sid = (r'source=energy_aspects'
                r' ea_data_service=crude_oil'
                r' economic_property=production'
                r' not energy_product_subtype=Condensate')


# Define routing/Endpoint
# OOP, having methods as keywords: GET, POST, PUT, DELETE
@api.route('/supply/crude')
class Crude(Resource, Base):
    @api.expect(parser_crude_supply_get)
    def get(self):
        """Returns the Crude Oil Supply data based on the parameters specified

        Returns:
        + JSON with Fields and Points data, having each Series in a column"""

        # Retrieve arguments
        args = parser_crude_supply_get.parse_args()
        country = args.get('country', None)
        country_iso = args['country_iso']
        region = args['region']
        date_from = args.get('date_from')
        date_to = args['date_to']

        # Check at least one identifier has been provided
        if not (country or country_iso or region):
            self.response['ERROR'] = 'Error: at least one identifier is required (country, country_iso, region)', 400

        # Set Identifier Precedence
        if country_iso:
            param_identifier = f' country_iso=({country_iso})'
        elif country:
            param_identifier = f' country=({country})'

        elif region:
            param_identifier = f' region=({region})'

        # Set date range - Date From
        if date_from:
            param_date_from = f'@df:{date_from}'
        else:
            param_date_from = ''

        # Set date range - Date To
        if date_to:
            param_date_to = f'@dt:{date_to}'
        else:
            param_date_to = ''

        # Prepare SID
        sid = self.base_sid + param_identifier + param_date_from + param_date_to

        # Retrieve multiple results/series
        fields, points = [], []
        for s in self.sj.scroll(sid, fields=self.fields, max_points=-1, serializer=shooju.points_serializers.pd_series):
            fields.append(s.get('fields'))
            points.append(s.get('points'))

        # Group multiple series in a Dataframe
        # Points
        df_points = pd.concat(points, axis=1)

        # Series
        df_fields = pd.DataFrame(fields).T  # Transpose

        # Merge Fields and Points, as SJ Excel result
        df = pd.concat([df_fields, df_points])

        # Rename Columns
        df.columns = [x['country'] for x in fields]

        # Format dataframe into JSON
        self.response = df.to_json(orient='columns', date_format='iso', indent=2)

        # TODO: DEBUGGING
        print(self.response)

        return self.response, 200


# api = Base()
#
# param = (r' country=(Algeria)'
#          r'@df:2018-01-01 @dt:2019-12-01')
#
# sid = api.base_sid + param
#
# # Retrieve multiple results/series
# fields, points = [], []
# for s in api.sj.scroll(sid, fields=api.fields, max_points=-1, serializer=shooju.points_serializers.pd_series):
#     fields.append(s.get('fields'))
#     points.append(s.get('points'))
#
# # Group multiple series in a Dataframe
# # Points
# df_points = pd.concat(points, axis=1)
#
# # Series
# df_fields = pd.DataFrame(fields).T  # Transpose
#
# df = pd.concat([df_fields, df_points])
# # Rename Columns
# df.columns = [x['country'] for x in fields]
#
# pass

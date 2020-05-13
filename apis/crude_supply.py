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
parser_crude_supply_get.add_argument('countries', type=str, required=False, help='DZ OR DZ,CG,...')
parser_crude_supply_get.add_argument('date_from', type=str, required=False, help='2017-01-01 OR -2y')
parser_crude_supply_get.add_argument('date_to', type=str, required=False, help='2030-01-01 OR +3M')

#TODO: how to retrieve latest, second latest etc (i.e: -1)
# there is a specific function for that -> conn.get_reported_date()
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
    # TODO: double-check desired meta fields to be returned
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
        countries = args['countries']
        date_from = args.get('date_from')
        date_to = args['date_to']
        report_date = args['report_date']

        # Set Identifier Precedence
        if countries:
            param_identifier = f' country_iso=({countries})'

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

        if report_date:
            param_report_date = f'@repdate:{report_date}'
        else:
            param_report_date = ''

        # Prepare SID
        sid = self.base_sid + param_identifier + param_date_from + param_date_to + param_report_date

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

        return self.response, 200


#TODO: Developing

# api = Base()
#
# param = (r' country=(Algeria,Congo,Venezuela,Kuwait)'
#          r'@df:2018-01-01 @dt:2019-12-01')
#
# sid = api.base_sid + param
#
# #
# # Retrieve multiple results/series
# fields, points, repdates = [], [], []
# for s in api.sj.scroll(sid, fields=api.fields, max_points=-1, serializer=shooju.points_serializers.pd_series):
#     # Extract available reported dates for each SID
#     repdate = api.sj.get_reported_dates(s['series_id'])
#
#     # Make sure report date is sorted ascending
#     repdate.sort()
#
#     # Convert to string date_iso format
#     repdate = [x.strftime('%Y-%m-%d') for x in repdate]
#
#     # Store as dictionary
#     repdate = {'repdate': repdate}
#
#     repdates.append(repdate)
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
# # List of Reported Dates
# df_repdate = pd.DataFrame(repdates).T  # Transpose
#
# df = pd.concat([df_fields, df_points, df_repdate])
# # Rename Columns
# df.columns = [x['country'] for x in fields]
#
# pass
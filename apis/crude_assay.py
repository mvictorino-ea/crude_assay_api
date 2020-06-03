from flask_restx import Namespace, Resource
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from core.utils import api_to_conversion_factor

# Initialize RestPlus* Extension API as Namespace
api = Namespace('crude_assay', description='Crude Assay operations')

### Swagger Documentation
# Define Endpoint-specific Parser
parser_crude_get = api.parser()
parser_crude_blend_get = api.parser()

# Define Endpoint Expected Parameters
parser_crude_get.add_argument('ids', type=str, required=True, help='1,2')

parser_crude_blend_get.add_argument('ids', type=str, required=True, help='1,2')
parser_crude_blend_get.add_argument('volumes', type=str, required=True, help='100,900')
###


class Base:
    # Load .env file
    load_dotenv()

    # Default Response
    response = {}

    # Default connection to database
    conn = psycopg2.connect(os.getenv('DATABASE_URL_PROD'))


# Define routing/Endpoint
@api.route('/data/reference/crudes')
# OOP, having methods as keywords: GET, POST, PUT, DELETE
class Crudes(Resource, Base):
    def get(self):
        """Returns all data in Crude Table

        Returns:
        + JSON with all data in Crude Table, row by row"""

        query = f"""SELECT * FROM crude AS c"""

        # Results from query
        df = pd.read_sql_query(query, self.conn)

        self.response = df.to_dict(orient='records')

        return self.response, 200


@api.route('/data/reference/crudes/')
class Crude(Resource, Base):
    @api.expect(parser_crude_get)
    def get(self):
        """Returns all Crude data for a specific list of Crude IDs

        Returns:
        + JSON with Crude and Location data, row by row"""

        # Retrieve arguments
        args = parser_crude_get.parse_args()
        ids = args['ids']

        # Transform into list
        ids = ids.split(',')

        if ids is None:
            self.response["ERROR"] = "Error: Missing required fields: ids."
            return self.response, 400

        # Check Crude ID is integer-like
        try:
            ids = np.array(ids, dtype=int)
        except TypeError:
            self.response['ERROR'] = 'Error: Crude IDs must be a number.'
            return self.response, 400

        # Prepare ids to be used in WHERE clause
        crude_ids = ', '.join([f"'{str(x)}'" for x in ids])

        query = f"""SELECT c.*, L.region, L.region_short, L.country, L.country_iso
                    FROM crude AS c
                    INNER JOIN location L ON c.location_id = L.location_id
                    WHERE c.crude_id IN ({crude_ids})"""

        # Results from query
        df = pd.read_sql_query(query, self.conn)

        self.response = df.to_dict(orient='records')

        return self.response, 200

    # TODO: placeholder for additional CRUD operations
    def post(self):
        """**Placeholder**: Add record to Crude Table"""

        self.response['ERROR'] = 'Error: Currently not developed'
        return self.response, 500

    # TODO: placeholder for additional CRUD operations
    def put(self):
        """**Placeholder**: Update a specific record in Crude Table"""

        self.response['ERROR'] = 'Error: Currently not developed'
        return self.response, 500

    # TODO: placeholder for additional CRUD operations
    def delete(self):
        """**Placeholder**: Delete a specific record in Crude Table"""

        self.response['ERROR'] = 'Error: Currently not developed'
        return self.response, 500


@api.route('/data/reporting/crude_blend/')
class CrudeBlend(Resource, Base):
    @api.expect(parser_crude_blend_get)  # Link parser documentation to specific endpoint
    def get(self):
        """Perform Weighted Average, Blending Crudes based on their respective volume

        Args:
        + **ids**: list of Crude IDs, comma separated
        + **volumes**: list of their respective volume(or ratio), comma separated

        Returns:
        + JSON with blended yields and the Conversion Factor, API, Sulphur Total %
        """

        response = {}

        # Retrieve arguments
        args = parser_crude_blend_get.parse_args()
        ids = args['ids']
        volumes = args['volumes']

        if ids is None or volumes is None:
            response["ERROR"] = "Error: Missing required fields: ids or volumes."
            return response, 400

        # Transform into list
        ids = ids.split(',')
        volumes = volumes.split(',')

        # Transform volume into list of numbers
        try:
            volumes = np.array(volumes, dtype=float)
        except Exception as e:
            response['ERROR'] = 'Error: Volumes must be a number.'
            return response, 400

        # Edge-case: warning that all volumes are zero
        if sum(volumes) == 0:
            response['ERROR'] = 'Warning: All volumes are zero.'
            return response, 400

        # Transform ids into list of integers
        try:
            ids = np.array(ids, dtype=int)
        except Exception as e:
            response['ERROR'] = 'Error: Crude IDs must be a number.'
            return response, 400

        # Make sure ids and volumes have same length
        if len(ids) != len(volumes):
            response["ERROR"] = "Error: Length of parameters do not match."
            return response, 400

        # Create Dataframe with requested parameters
        df = pd.DataFrame({'id': ids, 'volume': volumes})
        # Aggregate over ID (allow duplicate IDS)
        df_aggregated = df.groupby('id').sum().reset_index()

        # Return list of IDS and respective Volumes
        ids = df_aggregated['id'].values
        volumes = df_aggregated['volume'].values

        # Prepare ids to be used in WHERE clause
        crude_ids = ', '.join([f"'{str(x)}'" for x in ids])

        ### Actually perform Blending operation
        # Connect to database
        conn = psycopg2.connect(os.getenv('DATABASE_URL_PROD'))

        query_yield = f"""SELECT c.crude_id, c.name, p.product_name, a."yield_w_%", p."cut_start_C"
                    FROM crude AS c
                    INNER JOIN assay AS a
                        ON c.crude_id = a.crude_id
                    INNER JOIN product p ON a.product_id = p.product_id

                    WHERE c.crude_id IN ({crude_ids})
                        AND a.recommended IS TRUE
                        AND p.include_in_sum_yields IS TRUE
                    ORDER BY c.crude_id, p."cut_start_C" ASC"""

        # Results from query
        df = pd.read_sql_query(query_yield, conn)

        # Check if Crude ID has been found in Database
        crude_ids_identified = df.crude_id.unique()

        id_not_found = []
        if len(crude_ids_identified) < len(ids):
            for id in ids:
                if int(id) not in crude_ids_identified:
                    id_not_found.append(id)
            response['ERROR'] = f'Error: No recommended data found for the following Crude ID: {id_not_found}'

            return response, 400

        # Rearrange dataframe
        df = df.set_index(["crude_id", "name", 'product_name'])['yield_w_%'].unstack().reset_index()

        # Reorder columns to match expected result
        product_name_order = ['light_gasoline', 'light_naphtha', 'heavy_naphtha', 'kerosene', 'atm_gas_oil',
                              'light_vac_gas_oil', 'heavy_vac_gas_oil', 'vac_residue']
        column_order = ['crude_id', 'name'] + product_name_order

        df = df[column_order]

        # Keep only yields
        yields = df.iloc[:, 2:]

        # Weighted average
        crude_blend = np.average(yields, weights=volumes, axis=0)

        # Blending API and Sulphur Total %
        query_attributes = f"""SELECT c.crude_id, c.name, a.api, a."sulphur_total_%"
                            FROM crude AS c
                            INNER JOIN assay AS a
                                ON c.crude_id = a.crude_id
                            INNER JOIN product p ON a.product_id = p.product_id

                            WHERE c.crude_id IN ({crude_ids})
                                AND a.recommended IS TRUE
                                AND p.product_name = 'whole_crude'
                            ORDER BY c.crude_id, p."cut_start_C" ASC"""

        # Results from query
        whole_crude_attributes = pd.read_sql_query(query_attributes, conn)

        # Keep only attributes
        whole_crude_attributes = whole_crude_attributes[['api', 'sulphur_total_%']]

        # Weighted average
        whole_crude_attributes_blend = np.average(whole_crude_attributes, weights=volumes, axis=0)

        api, sulphur_total = whole_crude_attributes_blend[0], whole_crude_attributes_blend[1]

        # Store result in dataframe for easier export
        df_blended = pd.DataFrame(columns=product_name_order)
        df_blended.loc[0] = crude_blend

        # Add Blended whole crude attributes
        df_blended.loc[0, 'api'] = api

        df_blended.loc[0, 'sulphur_total_%'] = sulphur_total

        df_blended.loc[0, 'conversion_factor'] = api_to_conversion_factor(api)

        # Final response to be returned
        response = df_blended.iloc[0].to_dict()

        # Close connection
        conn.close()

        # Uncomment for debugging
        # return f'{df.to_html()} <br> {whole_crude_attributes.to_html()} <br> {df_blended.to_html()}'

        return response, 200

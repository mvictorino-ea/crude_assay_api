from flask import Flask, request, jsonify
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import numpy as np

# Load .env file
load_dotenv()

# Initialize App
app = Flask(__name__)

# Set URL to connect to Database
database_url = os.getenv("DATABASE_URL")


def api_to_conversion_factor(api):
    """Convert from API to Conversion Factor"""
    cf = 6.28981 / (141.5/(api + 131.5))
    return cf

# Welcome message
@app.route('/')
def index():
    return "<h1>Welcome to the Crude Assay API.</h1>"


@app.route('/api/v1/crude_blend', methods=['GET'])
def crude_blend():
    response = {}

    # Retrieve arguments
    ids = request.args.get('ids', None)
    volumes = request.args.get('volumes', None)

    if ids is None or volumes is None:
        response["ERROR"] = "Missing required fields: ids or volumes."
        return response, 500

    # Transform into list
    ids = ids.split(',')
    volumes = volumes.split(',')

    # Prepare ids to be used in WHERE clause
    crude_ids = ', '.join([f"'{str(x)}'" for x in ids])

    # Transform volume into list of numbers
    try:
        volumes = np.array(volumes, dtype=float)
    except Exception as e:
        response['ERROR'] = 'Volumes must be a number.'
        return response, 500

    # Make sure ids and volumes have same length
    if len(ids) != len(volumes):
        response["ERROR"] = "Length of parameters do not match."
        return response, 500

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

        return response, 500

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

    # column_order = ['crude_id', 'name', 'api', 'sulphur_total_%']
    #
    # whole_crude_attributes = whole_crude_attributes[column_order]

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


@app.route('/getmsg/', methods=['GET'])
def respond():
    # Retrieve the name from url parameter
    name = request.args.get("name", None)

    # For debugging
    print(f"got name {name}")

    response = {}

    # Check if user sent a name at all
    if not name:
        response["ERROR"] = "no name found, please send a name."
    # Check if the user entered a number not a name
    elif str(name).isdigit():
        response["ERROR"] = "name can't be numeric."
    # Now the user entered a valid name
    else:
        response["MESSAGE"] = f"Welcome {name} to our awesome platform!!"

    # Return the response in json format
    return jsonify(response)


@app.route('/post/', methods=['POST'])
def post_something():
    param = request.form.get('name')
    print(param)
    # You can add the test cases you made in the previous function, but in our case here you are just testing the POST functionality
    if param:
        return jsonify({
            "Message": f"Welcome {param} to our awesome platform!!",
            # Add this option to distinct the POST request
            "METHOD": "POST"
        })
    else:
        return jsonify({
            "ERROR": "no name found, please send a name."
        })


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    if os.getenv('ENVIRONMENT') == 'production':
        app.run(threaded=True)
    else:
        app.run(threaded=True, debug=True)

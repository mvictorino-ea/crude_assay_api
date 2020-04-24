# REST API: Heroku + Flask
Additional to the Crude Assay Database Web Application, this API will serve data from the database to other users (Excel).

# Required files to deploy to Heroku
### 1) Procfile
Tells Heroku what to do.

`web: gunicorn app:app`

Start the GUnicorn server and serve the Flask *app* (shortcut for application), in this case, `app.py`.

### 2) Requirements.txt (or Pipfile)
By default, newly created Python apps in Heroku will use the python-3.6.10 runtime (as of March 2020). You can specify
the Python runtime version to be used in order to have more control. See below for instructions.

#### 2.a) Pipenv
If using `Pipenv` to install packages in the virtual environment, the `Pipfile` will already be available, specifying
the required packages, versions, and Python version.

#### 2.b) Other Virtual Environment
If using other approach (i.e. `venv`), required to create a `requirements.txt` and specify the Python version to load
on Heroku:

`pip freeze > requirements.txt`

> **IMPORTANT**: if using Windows, necessary to remove `winpy32` from requirements.txt file (otherwise Linux VM won't build)
>
**runtime.txt**

According to Heroku's documentation, the `runtime.txt` file must contain the Major, Minor, and Patch version, without
any spaces. In my case:

`python-3.7.6`

### 3) Allow database connection from Heroku App
This was a very confusing topic, with many small variations to Stack Overflow answers and even Heroku's recommendations.

The steps presented in this [Stack Overflow Answer](https://stackoverflow.com/questions/35247347/point-heroku-application-to-aws-rds-database)
worked for me, with a tiny difference in the `DATABASE_URL` variable:

+ Follow all detailed steps on the AWS side, creating a Postgresql database, setting its configurations etc.
+ Download the root_certificate available to all regions, from [AWS docs](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html) (rds-ca-2019-root.pem).
+ Place this `.pem` file in the project root folder
+ On Heroku, under the **settings** tab, set config vars (environment variable):
`DATABASE_URL=postgresql://{user}:{password}@{host}:{port}/{dbname}?ssl=true&sslrootcert={root_certificate_name}`

Then you can use the `database_url = os.getenv("DATABASE_URL")` to create a connection in the code, `conn = psycopg2.connect(os.getenv('DATABASE_URL'))`.
This `conn` feeds into the `pandas.read_sql_query`.

> Note: the environment variable "DATABASE_URL" is stored in Heroku under Settings > Config Vars 


### 4) Excel Function Example
More information regarding the Excel function can be found in [the documentation](https://github.com/mvictorino-ea/crude_assay_api/blob/master/Excel_VBA/documentation_VBA.md)

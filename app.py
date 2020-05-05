from flask import Flask
import os
from dotenv import load_dotenv
from apis import api  # Aggregated API object, from __init__ file


# Load .env file
load_dotenv()

# Initialize App
app = Flask(__name__)

# Link Aggregated API object to Flask APP, so it will be served
api.init_app(app)


if __name__ == '__main__':
    # Threaded option to enable multiple instances for multiple user access support
    if os.getenv('ENVIRONMENT') == 'production':
        app.run(threaded=True)
    else:
        app.run(threaded=True, debug=True)

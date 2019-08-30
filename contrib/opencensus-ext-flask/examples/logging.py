import logging

from flask import Flask
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.ext.flask.flask_middleware import FlaskMiddleware

application = Flask(__name__)

logger = logging.getLogger(__name__)
handler = AzureLogHandler(instrumentation_key='your-instrumentation-key-here')
logger.addHandler(handler)

middleware = FlaskMiddleware(application)

@application.route("/")
def hello():
    logger.warning("Testing Logs!")
    return "<h1 style='color:blue'>Hello There!</h1>"

if __name__ == "__main__":
    application.run(host='localhost', port=8080, threaded=True)

from flask import Flask
from opencensus.ext.flask.flask_middleware import FlaskMiddleware
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace.samplers import ProbabilitySampler
import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler

application = Flask(__name__)

# logger = logging.getLogger(__name__)
# handler = AzureLogHandler(instrumentation_key='70c241c9-206e-4811-82b4-2bc8a52170b9')
# logger.addHandler(handler)

exporter = AzureExporter(instrumentation_key='70c241c9-206e-4811-82b4-2bc8a52170b9')
sampler=ProbabilitySampler(1.0)
middleware = FlaskMiddleware(application, sampler=sampler)

@application.route("/")
def hello():
#     logger.warning("Testing!")
    return "<h1 style='color:blue'>Hello There!</h1>"

if __name__ == "__main__":
    application.run(host='localhost', port=8080, threaded=True)

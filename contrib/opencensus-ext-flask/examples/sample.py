from flask import Flask
from opencensus.ext.flask.flask_middleware import FlaskMiddleware
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace.samplers import ProbabilitySampler

application = Flask(__name__)

exporter = AzureExporter(instrumentation_key='70c241c9-206e-4811-82b4-2bc8a52170b9')
sampler=ProbabilitySampler(1.0)
middleware = FlaskMiddleware(application)

@application.route("/")
def hello():
    return "<h1 style='color:blue'>Hello There!</h1>"

if __name__ == "__main__":
    application.run(host='localhost', port=8080, threaded=True)


########################################################
# Setup logging and stream with this code,
# then insert data in the table using this query:
#
#
# INSERT INTO ops.TestTraces
# SELECT coalesce(max(ID),0) + 1 from ops.TestTraces
#
#############################################################

import time
import os
import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace import config_integration
from opencensus.trace.samplers import ProbabilitySampler
from opencensus.trace.tracer import Tracer
# from pyspark.sql import functions as F
from opencensus.trace import execution_context
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import TimestampType, StringType, StructType, StructField

# logger for app insights library
logger = logging.getLogger(__name__)

def setupLogging(loggerName):
    """
        Setup Logging to transfer to Azure application Insights
    """
    # general login setup
    common_logging_format = '%(levelname)s - %(name)s - %(message)s'
    basic_logging_format = '%(asctime)s - ' + common_logging_format
    config_integration.trace_integrations(['logging'])
    logging.basicConfig(format=basic_logging_format, level=logging.DEBUG)
    # logging.getLogger("py4j").setLevel(logging.ERROR)
    # logging.getLogger("opencensus").setLevel(logging.DEBUG)
    # get the logger for the name provided
    new_logger = logging.getLogger(loggerName)
    # opencesus config links logging to ApplicationInsights
    # try:
        # instrumentation_key = os.environ.get("APPINSIGHTS_INSTRUMENTATIONKEY")
        # logger.info('App Insight instrumentation Key: {instrumentation_key}')
    app_insights_handler = AzureLogHandler()
    app_insights_handler.setFormatter(logging.Formatter(common_logging_format))
    new_logger.addHandler(app_insights_handler)
        # also add handler for this library and other datahub.* libraries
        # logging.getLogger('datahub').addHandler(app_insights_handler)
        # logger.info(f'Added App Insight logging handler: {app_insights_handler}')
    # except:
    #     logger.error(f'No environement variable or wrong format for: APPINSIGHTS_INSTRUMENTATIONKEY={instrumentation_key}')
    # return logger to use in main etl
    return new_logger


def setupTracer(loggerName):
    tracer = Tracer(
        exporter=AzureExporter(),
        sampler=ProbabilitySampler(1.0)
    )
    return tracer


spark = SparkSession.builder \
        .master("local") \
        .appName("StreamingApp") \
        .getOrCreate()

# spark.sql("CREATE TABLE ops.TestTraces (ID INTEGER) USING DELTA")
# STORAGE_ACCOUNT = os.environ.get("STORAGE_ACCOUNT")
# CHECKPOINT_LOCATION = f"abfss://ops@{STORAGE_ACCOUNT}/checkpoint/test_traces"

LOGGER = setupLogging('Logger name')
TRACER = setupTracer('Tracer name')

# Path to our 20 JSON files
inputPath = "./streaming/"


def test_logger(str_var):
  with TRACER.span(name=f'test_logger'):
    LOGGER.info(f"LOGGER test {str_var}")
    
def for_each_batch(batch_df, batch_id):
    with TRACER.span(name=f'for_each_batch'):
    # id_list = batch_df.agg(F.collect_list("ID").alias("IDs")).first().IDs
    # id_string = ", ".join(str(x) for x in id_list)
    # test_logger("for_each_batch")
        execution_context.set_opencensus_full_context(TRACER, TRACER.current_span(), execution_context.get_opencensus_attrs())
        LOGGER.info(f"In for each batch")
    # LOGGER.info(f"Processed Ids : ")


def main():
    spark.sql("set spark.sql.streaming.schemaInference=true")
    with TRACER.span(name=f'main'):
        LOGGER.info(f"Main before")
        streamingDF = spark.readStream.option("maxFilesPerTrigger", 1).json(inputPath)
        # Stream `streamingDF` while aggregating by action
        streamingActionCountsDF = (
            streamingDF
                .groupBy(
                streamingDF.action
                )
                .count()
        )
        spark.conf.set("spark.sql.shuffle.partitions", "2")
        execution_context.set_opencensus_attr("test", "value")
        query = (
            streamingActionCountsDF
                .writeStream
                .format("memory")
                .queryName("counts")
                .outputMode("complete")
                .foreachBatch(for_each_batch)
                .start().awaitTermination(10)
        )
        # spark.readStream.format("delta").option("ignoreChanges", "true").table("ops.TestTraces") \
        #     .writeStream.option("checkpointLocation", CHECKPOINT_LOCATION).foreachBatch(for_each_batch).start() \
        #     .awaitTermination()


if __name__ == "__main__":
    main()

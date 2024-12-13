import os
import findspark

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/home/jeremie/Downloads/Spark"

findspark.init()

from pyspark.sql import SparkSession

def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession \
            .builder \
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .master('local[*]') \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession \
            .builder \
            .enableHiveSupport() \
            .getOrCreate()
    
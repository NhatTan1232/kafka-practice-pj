import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

import util.config as conf
from util.logger import Log4j

if __name__ == '__main__':
    working_dir = os.getcwd()
    print("working_dir: " + working_dir)

    spark_conf = conf.get_spark_conf()

    spark = SparkSession \
        .builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    surveyDf = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/data/spark-sql/survey.csv")

    surveyDf.createOrReplaceTempView("survey_view")
    countDf = spark.sql(
        """
        select Country, count(1) as Male_number
        from survey_view 
        where Age < 40 and Gender in ("Male", "M")
        group by Country
        order by Male_number desc, Country
        """)

    log.info("countDf: ")
    countDf.show()

    spark.stop()

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
        select 
            Country, 
            count(case when lower(Gender) in ('m', 'male') then 1 end) AS Male_count,
            count(case when lower(Gender) in ('female', 'w', 'women') then 1 end) AS Female_count
        from survey_view 
        group by Country
        order by Country
        """)

    log.info("countDf: ")
    countDf.show()

    spark.stop()

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

@dp.materialized_view(
#   format= 'delta',             # defines a delta table
  name="demo.sdp_bronze.customer_silver",  # in demo catalog in Unity Catalog
  table_properties={
    # Managed table creation requires table property 'delta.feature.catalogManaged'='supported' to be set.
    'delta.feature.catalogManaged': 'supported',
  }
)
def silver_customer():

    return (
        spark.read.table("demo.sdp_bronze.customers_bronze")
        .dropDuplicates(["c_customer_sk"])
        .filter(F.col("c_customer_id").isNotNull())
        .withColumn(
            "c_email_address",
            F.lower(F.trim("c_email_address"))
        )
        .withColumn(
            "customer_age",
            F.year(F.current_date()) - F.col("c_birth_year")
        )
        .withColumn(
            "age_group",
            F.when(F.col("customer_age") < 25, "Young")
             .when(F.col("customer_age") < 40, "Adult")
             .when(F.col("customer_age") < 60, "Middle Aged")
             .otherwise("Senior")
        )
    )
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

@dp.materialized_view(
#   format= 'delta',             
  name="demo.sdp_bronze.item_silver",  
  table_properties={
    'delta.feature.catalogManaged': 'supported',
  }
)
def silver_item():

    return (
        spark.read.table("demo.sdp_bronze.item_bronze")
        .dropDuplicates(["i_item_sk"])
        .filter(F.col("i_item_id").isNotNull())
        .filter(F.col("i_current_price") > 0)
        .withColumn(
            "i_category",
            F.initcap(F.trim("i_category"))
        )
        .withColumn(
            "price_band",
            F.when(F.col("i_current_price") < 1000, "Budget")
             .when(F.col("i_current_price") < 5000, "Mid Range")
             .when(F.col("i_current_price") < 15000, "Premium")
             .otherwise("Luxury")
        )
    )   
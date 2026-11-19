from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

@dp.materialized_view(
#   format= 'delta',             
  name="demo.sdp_bronze.store_sales_silver",  
  table_properties={
    'delta.feature.catalogManaged': 'supported',
  }
)
def silver_store_sales():

    return (
        spark.read.table("demo.sdp_bronze.stores_sales_bronze")
        .dropDuplicates([
            "ss_ticket_number",
            "ss_item_sk",
            "ss_sold_date_sk"
        ])
        .filter(F.col("ss_quantity") > 0)
        .filter(F.col("ss_sales_price") > 0)
        .withColumn(
            "calculated_sales",
            F.round(
                F.col("ss_quantity") * F.col("ss_sales_price"),
                2
            )
        )
        .withColumn(
            "discount_amount",
            F.round(
                F.col("calculated_sales") - F.col("ss_net_paid"),
                2
            )
        )
        .withColumn(
            "discount_percentage",
            F.when(
                F.col("calculated_sales") > 0,
                F.round(
                    F.col("discount_amount") /
                    F.col("calculated_sales") * 100,
                    2
                )
            ).otherwise(0)
        )
    )
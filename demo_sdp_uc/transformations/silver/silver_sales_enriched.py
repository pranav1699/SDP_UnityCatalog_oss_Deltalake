from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

@dp.materialized_view(
#   format= 'delta',             
  name="demo.sdp_bronze.silver_sales_enriched",  
  table_properties={
    'delta.feature.catalogManaged': 'supported',
  }
)
def silver_sales_enriched():

    sales = spark.read.table("demo.sdp_bronze.store_sales_silver").alias("ss")
    customers = spark.read.table("demo.sdp_bronze.customer_silver").alias("c")
    items = spark.read.table("demo.sdp_bronze.item_silver").alias("i")
    stores = spark.read.table("demo.sdp_bronze.stores_bronze").alias("s")

    return (
        sales
        .join(
            customers,
            F.col("ss.ss_customer_sk") == F.col("c.c_customer_sk"),
            "left"
        )
        .join(
            items,
            F.col("ss.ss_item_sk") == F.col("i.i_item_sk"),
            "left"
        )
        .join(
            stores,
            F.col("ss.ss_store_sk") == F.col("s.s_store_sk"),
            "left"
        )
        .select(
            F.col("ss.ss_ticket_number"),
            F.col("ss.ss_sold_date_sk"),
            F.col("ss.ss_quantity"),
            F.col("ss.ss_sales_price"),
            F.col("ss.ss_ext_sales_price"),
            F.col("ss.ss_net_paid"),
            F.col("ss.calculated_sales"),
            F.col("ss.discount_amount"),
            F.col("ss.discount_percentage"),
            F.col("c.c_customer_id"),
            F.col("c.c_first_name"),
            F.col("c.c_last_name"),
            F.col("c.age_group"),
            F.col("i.i_item_id"),
            F.col("i.i_item_desc"),
            F.col("i.i_category"),
            F.col("i.price_band"),
            F.col("s.s_store_name"),
            F.col("s.s_city"),
            F.col("s.s_state")
        )
    )

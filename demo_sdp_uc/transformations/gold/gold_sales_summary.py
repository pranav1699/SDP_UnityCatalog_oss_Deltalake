from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


@dp.materialized_view(
    name="demo.sdp_bronze.gold_sales_summary",
    table_properties={
        "delta.feature.catalogManaged": "supported",
    },
)
def gold_sales_summary():
    sales = spark.read.table("demo.sdp_bronze.silver_sales_enriched")

    return (
        sales.groupBy(
            "ss_sold_date_sk",
            "s_store_name",
            "s_city",
            "s_state",
            "i_category",
        )
        .agg(
            F.sum("ss_quantity").alias("units_sold"),
            F.countDistinct("ss_ticket_number").alias("transaction_count"),
            F.round(F.sum("calculated_sales"), 2).alias("gross_sales"),
            F.round(F.sum("ss_net_paid"), 2).alias("net_sales"),
            F.round(F.sum("discount_amount"), 2).alias("total_discount"),
            F.round(F.avg("discount_percentage"), 2).alias("avg_discount_percentage"),
        )
    )

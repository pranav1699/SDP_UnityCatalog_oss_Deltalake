from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()


@dp.materialized_view(
    name="demo.sdp_bronze.gold_customer_analytics",
    table_properties={
        "delta.feature.catalogManaged": "supported",
    },
)
def gold_customer_analytics():
    sales = spark.read.table("demo.sdp_bronze.silver_sales_enriched")

    return (
        sales.groupBy(
            "c_customer_id",
            "c_first_name",
            "c_last_name",
            "age_group",
        )
        .agg(
            F.countDistinct("ss_ticket_number").alias("transaction_count"),
            F.sum("ss_quantity").alias("units_purchased"),
            F.countDistinct("i_item_id").alias("distinct_items_purchased"),
            F.countDistinct("i_category").alias("distinct_categories_purchased"),
            F.round(F.sum("calculated_sales"), 2).alias("gross_sales"),
            F.round(F.sum("ss_net_paid"), 2).alias("net_sales"),
            F.round(F.sum("discount_amount"), 2).alias("total_discount"),
            F.round(F.avg("discount_percentage"), 2).alias("avg_discount_percentage"),
        )
    )

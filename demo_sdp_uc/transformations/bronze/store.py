from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.active()

@dp.table(
#   format= 'delta',             # defines a delta table
  name="demo.sdp_bronze.stores_bronze",  # in demo catalog in Unity Catalog
  table_properties={
    # Managed table creation requires table property 'delta.feature.catalogManaged'='supported' to be set.
    'delta.feature.catalogManaged': 'supported',
  }
)
def stores_bronze() -> DataFrame:
    return (
        spark
        .readStream
        .table("demo.sdp.store")
    )
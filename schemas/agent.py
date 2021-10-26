
from pyspark.sql.types import StructField, StringType, NumericType, StructType, TimestampType

def load_agent():
    schema = StructType([
        StructField("id", NumericType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("created_by", NumericType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("updated_by", NumericType(), True),
        # StructField("deleted_at", LongType(), True),
    ])
    return schema
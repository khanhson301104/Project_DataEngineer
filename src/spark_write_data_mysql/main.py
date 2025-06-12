from config.spark_config import get_spark_config, SparkConnect
from pyspark.sql.functions import col
from config.database_config import get_database_config
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from src.spark_write_data_mysql.spark_write_data import SparkWriteDatabase

def main():
    spark_config = get_spark_config()
    database_config = get_database_config()

    jars = [database_config["mysql"].jar_path]

    spark_connect = SparkConnect(
        "GoldPrice",
        master_url='local[*]'
        , executor_memory='4g'
        , executor_cores=2
        , driver_memory='2g'
        , num_executor=3
        , jars=jars
        , log_level='INFO')

    schema = StructType([
        StructField("LOAIVANG", StringType(), nullable=False),
        StructField("GIAMUA", StringType(), nullable=False),
        StructField("GIABAN",StringType(), nullable=False),
        StructField("NGAY", StringType(), nullable=False),
        StructField("THANHPHO", StringType(), nullable=False)
    ])

    df = spark_connect.spark.read.option("multiLine", "true").schema(schema).json(r"D:\Project_DataEngineer\temp_json_data\gold_price_data.json")
    df = df.withColumn("spark_temp",lit("sparkwrite")).select("LOAIVANG", "GIAMUA", "GIABAN", "NGAY", "THANHPHO", "spark_temp")
    df_write = SparkWriteDatabase(spark_connect.spark, spark_config)
    # Hoi anh Dat vi sao minh truyen vao df ma van ghi thanh cong
    df_write.write_data_to_all(df,"append")

    df_validate = SparkWriteDatabase(spark_connect.spark,spark_config)
    df_validate.validate_data_mysql(df,spark_config["mysql"]["jdbc_url"],spark_config["mysql"]["table"],spark_config["mysql"]["config"])


main()


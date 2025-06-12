from config.spark_config import get_spark_config
from config.database_config import get_database_config
from config import spark_config
from pyspark.sql import DataFrame, SparkSession
from typing import Dict

from database_connect_schema.mysql_connect import MYSQL_CONNECT


class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, spark_config: Dict ):
        self.spark = spark
        self.spark_config = spark_config
    def write_data_to_mysql(self, df_write:DataFrame,table_name, jdbc_url,config:Dict, mode:str = "append"):
        db_config = get_database_config()
        with MYSQL_CONNECT(db_config["mysql"].host, db_config["mysql"].port, db_config["mysql"].user,db_config["mysql"].password) as mysql_client:
            connection, cursor = mysql_client.connection, mysql_client.cursor
            database = "gold_price"
            connection.database = database
            command = f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)"
            cursor.execute(command)
            connection.commit()
            mysql_client.close()
        df_write.write\
            .format("jdbc")\
            .option("url",jdbc_url)\
            .option("dbtable", table_name)\
            .option("user",config["user"])\
            .option("password", config["password"])\
            .mode(mode)\
            .save()
        print(f"Write data success to {table_name}")
    def validate_data_mysql(self, df_write:DataFrame, jdbc_url,table_name: str, config:Dict, mode: str = "append"):
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'sparkwrite') AS subquery") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .load()
        def subtract_dataframe(spark_write_to_database: DataFrame, dataframe_read_from_database: DataFrame):
            result = spark_write_to_database.exceptAll(dataframe_read_from_database)
            result.show()
            result.write\
            .format("jdbc")\
            .option("url",jdbc_url)\
            .option("dbtable", table_name)\
            .option("user",config["user"])\
            .option("password", config["password"])\
            .mode(mode)\
            .save()
        print(f"df read: {df_read.count()}, df write :{df_write.count()}")
        if df_read.count() == df_write.count():
            print(f"-----Validate {df_write.count()} records successfully write to database")
            subtract_dataframe(df_write,df_read)
            print(f"-----Validate data successfully")
        else:
            print("-----Validate missing data-----")
            subtract_dataframe(df_write, df_read)
            print(f"-----Insert missing records-----")

    def write_data_to_all(self, df:DataFrame, mode):
        self.write_data_to_mysql(
            df,
            self.spark_config["mysql"]["table"],
            self.spark_config["mysql"]["jdbc_url"],
            self.spark_config["mysql"]["config"],
            mode
        )


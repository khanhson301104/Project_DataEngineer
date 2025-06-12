from typing import Optional, Dict, List
from pyspark.sql import SparkSession
from config.database_config import get_database_config
import os

class SparkConnect:
    def __init__(self,
                 appname: str,
                 master_url: str,
                 executor_memory:Optional[str] = "2g",
                 executor_cores: Optional[int] = 1,
                 driver_memory:Optional[str] = "2g",
                 num_executor:Optional[int] = 3,
                 jars: Optional[List[str]] = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 log_level: str = "WARN"
    ):
        self.appname = appname
        self.spark = self.create_spark_session(master_url=master_url, executor_memory=executor_memory, executor_cores=executor_cores,driver_memory=driver_memory,
                                               num_executor=num_executor,jars=jars, spark_conf=spark_conf,log_level=log_level)
    def create_spark_session(
            self,
            master_url: str,
            executor_memory: Optional[str] = "2g",
            executor_cores: Optional[int] = 1,
            driver_memory: Optional[str] = "2g",
            num_executor: Optional[int] = 3,
            jars: Optional[str] = None,
            spark_conf: Optional[Dict[str, str]] = None,
            log_level: str = "WARN"
            ) -> SparkSession :
                builder = SparkSession.builder\
                    .appName(self.appname)\
                    .master(master_url)
                if executor_memory:
                    builder.config("spark.executor.memory", executor_memory)
                if executor_cores:
                    builder.config("spark.executor.cores", executor_cores)
                if driver_memory:
                    builder.config("spark.driver.memory", driver_memory)
                if num_executor:
                    builder.config("spark.executor.instances", num_executor)
                if jars:
                    jar_path =",".join([os.path.abspath(jar) for jar in jars])
                    builder.config("spark.jar", jar_path)
                if spark_conf:
                    for key,value in spark_conf.items():
                        builder.config(key, value)
                spark = builder.getOrCreate()
                spark.sparkContext.setLogLevel(logLevel=log_level)
                print("Create Spark Successfully")
                return spark
    def stop_spark(self):
        if self.spark:
            self.spark.stop()
            print("Stop SparkSession")


def get_spark_config() -> Dict:
    db_config = get_database_config()
    return {
        "mysql":{
            "table":db_config["mysql"].table,
            "jdbc_url": "jdbc:mysql://{}:{}/{}".format(db_config["mysql"].host,db_config["mysql"].port, db_config["mysql"].database),
            "config":{
                "host":db_config["mysql"].host,
                "port":db_config["mysql"].port,
                "user":db_config["mysql"].user,
                "password":db_config["mysql"].password,
                "database":db_config["mysql"].database
            }
        }
    }

import os
from typing import Dict, Optional
from dotenv import load_dotenv
from dataclasses import dataclass

@dataclass()
class DatabaseConfig:
    def validate_database(self):
        for key,value in self.__dict__.items():
            if value is None:
                raise ValueError(f"-----Missing value for {key}------")
@dataclass()
class MYSQL_DATABASE(DatabaseConfig):
    host:str
    port:int
    user:str
    password:str
    database:str
    jar_path:Optional[str]=None
    table:str="GOLDPRICE"
def get_database_config() -> Dict[str,DatabaseConfig]:
    load_dotenv()
    config = {
        "mysql":MYSQL_DATABASE(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            jar_path=os.getenv("MYSQL_JARPATH")
        )
    }

    for database, setting in config.items():
        setting.validate_database()
    return config


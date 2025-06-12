from config.database_config import get_database_config
from database_connect_schema.mysql_connect import MYSQL_CONNECT
from database_connect_schema.schema_manager import mysql_schema_manager, validate_mysql_schema

def main(config):
    with MYSQL_CONNECT(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
        connection,cursor = mysql_client.connection, mysql_client.cursor
        mysql_schema_manager(connection, cursor)
        cursor.execute("INSERT INTO GOLDPRICE (LOAIVANG, GIAMUA, GIABAN,NGAY,THANHPHO) VALUES (%s, %s, %s, %s, %s)", ("VANG SJC",15000, 16000,"2025-06-04","Hà Nội"))
        connection.commit()
        print("----INSERT Successfully----")
        validate_mysql_schema(cursor)

if __name__ == "__main__":
    config = get_database_config()
    main(config)



from pathlib import Path
from mysql.connector import Error
SQL_SCHEMA_PATH = Path("../sql/schema_database.sql")
def mysql_schema_manager(connection, cursor):
    database = "gold_price"
    cursor.execute("DROP DATABASE gold_price")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    print(f"Create new Database: {database} successfully")
    connection.database = database
    try:
        with open(SQL_SCHEMA_PATH, "r") as file:
            sql_script = file.read()
            sql_command = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]
            for cmd in sql_command:
                cursor.execute(cmd)
                print(f"Cursor execute command:{cmd}")
            connection.commit()
            print("----Create MySQL SCHEMA successfully----")
    except Error as e:
        connection.rollback()
        raise Exception("Fail")

def validate_mysql_schema(cursor):
    print("----Validate Tables----")
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    if "GOLDPRICE" not in tables:
        raise ("----Table GOLDPRICE does not exists----")
    print("----Validate Value----")
    cursor.execute("SELECT * FROM GOLDPRICE where THANHPHO = 'Hà Nội'")
    city = cursor.fetchone()
    if not city:
        raise ("----City not found----")
    print("----Validation successfully----")






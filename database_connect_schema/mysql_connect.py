import mysql.connector
from mysql.connector import Error

class MYSQL_CONNECT:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.config = {'host': self.host, 'port': self.port,'user': self.user, 'password': self.password}
        self.connection = None
        self.cursor = None
    def connect_to_mysql(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor()
            if self.connection:
                print("Connect to MYSQL successfully")
            return self.connection, self.cursor
        except Error as e:
            print(f"Get Error: {e}")
    def __enter__(self):
        self.connect_to_mysql()
        return self
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MYSQL CLOSE")
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()




import mysql.connector
from utils.con_parse import config_parse


class Database:

    def __init__(self, file):
        self.file = file
        self.connection = None
        self.cursor = None

    def connect(self):
        config = config_parse(self.file)
        db_host = config['host']['name']
        db_user = config['user']['name']
        db_pass = config['password']['pw']
        db_db = config['database']['db']
        self.connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_db
        )
        self.cursor = self.connection.cursor()

    def insert_data(self, table, data):
        query = f'INSERT INTO {table} VALUES (%s)'
        values = (data,)
        self.cursor.execute(query, values)
        self.connection.commit()

from BaseAdapter import BaseAdapter
import psycopg2

class PostgresAdapter(BaseAdapter):
    def __init__(self, db_name, db_user, db_host, db_password) -> None:
        super().__init__(db_name, db_user, db_host, db_password)
        self.conn = psycopg2.connect(database=self.db_name, user=self.db_user, host=self.db_host, password=self.db_password)

if __name__ == '__main__':
    DB = PostgresAdapter('dev', 'postgres', "localhost", 'admin')
    print(DB.read('iuser', "user_name = 'Elias'"))
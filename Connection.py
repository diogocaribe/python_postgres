import psycopg2 as pg

import yaml


class Connection:
    """Conection class."""
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.query = query
        self.conn = None

    def open_connect(self):
        """ Connect to the PostgreSQL database server """
        conn = self.conn
        
        try:
            # read connection parameters
            params = self.connection_string

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = pg.connect(params)
            conn.autocommit = True

            return conn
        except (Exception, pg.DatabaseError) as error:
            print(error)
    
    def close_connect(self):
        """ Close connect to the PostgreSQL database server """
        conn = self.conn
        try:
            conn.close()
        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
    
    def run_query(self, query):
        conn = self.open_connect()
        cursor = conn.cursor()
        cursor.execute(self.query)
        result = cursor.fetchall()
        cursor.close()
        return result


if __name__ == "__main__":
    with open('const.yaml', 'r') as f:
        p = yaml.load(f, Loader=yaml.FullLoader)
        query = 'SELECT * FROM foco_calor LIMIT 10'
        q = Connection(p)
        q.run_query(query)

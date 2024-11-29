import psycopg2

class DatabaseConnection:
    def __init__(self, dbname, user, password, host='localhost'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def connect(self):
        """Establish a database connection and return the cursor."""
        conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host
        )
        return conn, conn.cursor()
    
    

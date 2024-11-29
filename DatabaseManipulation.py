from DB_connection import *

class DatabaseManipulation:
    def __init__(self, db_connection, name: str):
        self.db_connection = db_connection
        self.admin_name = name


    def execute_query(self, query, params=None, fetch=False):   
        conn, cursor = self.db_connection.connect()
        try:
            cursor.execute(query, params)  # Use parameters with the query
            if fetch:
                return cursor.fetchall()  # Return fetched results if requested
            conn.commit()  # Commit changes to the database
        except Exception as e:
            conn.rollback()  # Roll back transaction on error
            raise e  # Re-raise the exception to handle it further up the call stack
        finally:
            cursor.close()
            conn.close()

db_conn = DatabaseConnection('assets_project_db', 'postgres', '6987129457', 'localhost')
master = DatabaseManipulation(db_conn, "Raul")
master.execute_query("""
    CREATE TABLE IF NOT EXISTS assets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(255) NOT NULL,
    total_amount INTEGER DEFAULT 0
);
""")


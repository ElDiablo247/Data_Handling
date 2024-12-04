from DB_connection import *

class DatabaseManipulation:
    def __init__(self, db_connection, name: str):
        self.db_connection = db_connection
        self.admin_name = name


    def execute_many(self, query, params, fetch=False):   
        conn, cursor = self.db_connection.connect()
        try:
            cursor.executemany(query, params)  
            if fetch:
                return cursor.fetchall() 
            conn.commit()  
        except Exception as e:
            conn.rollback()  
            raise e  
        finally:
            cursor.close()
            conn.close()

    def execute_query(self, query, params=None, fetch=False):
        conn, cursor = self.db_connection.connect()
        try:
            if params is not None:
                cursor.execute(query, params)  # Use parameters with the query
            else:
                cursor.execute(query)  # Execute without parameters if none provided
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

import mysql.connector
from mysql.connector import Error
import pandas as pd
from csv import reader


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")
        
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        #print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")

def convert_to_df(results,columns):
    from_db = []
    for result in results:
        result = list(result)
        from_db.append(result)
    df = pd.DataFrame(from_db, columns=columns)
    return(df)

def add_data_from_df(df, table, connection, dtype_list):
    #connection_replica = create_db_connection("localhost", "root", pw, database)
    for i in range(0,len(df)):
        #print(df.iloc[i,].values)
        data = tuple([x[1](x[0]) for x in zip(df.iloc[i,].values,dtype_list)])
        #print(dtypes_list[5](df.iloc[i,].values[5]))
        data = str(data)
        data = data.replace('0 days ','').replace('1 days ','')
        #data = data.replace("'NULL'","NULL")
        # if 'NULL' in data:
        #     print(data)
        populate = """
        INSERT INTO """ + table + """ VALUES
        """ + data + ';'
        #print(populate)
        execute_query(connection, populate)
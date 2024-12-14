import os
import msaccessdb
import numpy as np
import pyodbc
import pandas as pd

from src.entity.ticket_raw_entity import TicketRawEntity

# All functions in this file are used to interact with the Access database.

def create_database_if_not_exists(database_path):
    """
    Check if the database exists, create it if not.
    """
    # Ensure the directory exists
    directory = os.path.dirname(database_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directory created at {directory}")

    if not os.path.exists(database_path):
        msaccessdb.create(database_path)
        conn_str = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            fr"DBQ={database_path};"
        )
        conn = pyodbc.connect(conn_str, autocommit=True)
        print(f"Database created at {database_path}")
        return conn

    return None

def connect_to_database(database_path):
    """
    Connect to the specified Access database.
    """
    create_database_if_not_exists(database_path)
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        fr"DBQ={database_path};"
    )
    return pyodbc.connect(conn_str)

def get_table_list(conn):
    """
    Get a list of all tables in the database.
    """
    cursor = conn.cursor()
    cursor.tables()
    tables = [row.table_name for row in cursor if row.table_type == 'TABLE']
    cursor.close()
    return tables

def get_tables_and_size(conn):
    """
    Get a list of all tables in the database and their sizes.
    """
    cursor = conn.cursor()
    cursor.tables()
    tables = {row.table_name: get_table_size(conn, row.table_name) for row in cursor if row.table_type == 'TABLE'}
    cursor.close()
    return tables

def get_table_schema(conn, table_name):
    """
    Get the schema of the specified table.
    """
    cursor = conn.cursor()
    cursor.columns(table=table_name)
    schema = {row.column_name: row.type_name for row in cursor}
    cursor.close()
    return schema

def get_table_size(conn, table_name):
    """
    Get the size of the specified table.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    size = cursor.fetchone()[0]
    cursor.close()
    return size

def create_table_from_entity(conn, table_name, entity):
    """
    Create table based on the specified entity.
    """
    cursor = conn.cursor()
    if table_name not in get_table_list(conn):
        columns = []
        for col, value in entity.__dict__.items():
            if isinstance(value, pd.Timestamp) or col.lower().endswith('date') or col.lower().endswith('Yearmonth'):
                columns.append(f"[{col}] DATETIME NULL")
            else:
                columns.append(f"[{col}] TEXT NULL")
        column_definitions = ", ".join(columns)
        create_table_query = f"CREATE TABLE {table_name} ({column_definitions})"
        cursor.execute(create_table_query)
        conn.commit()
    cursor.close()

def get_table_data(conn, table_name):
    """
    Read data from the specified table.
    """
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    return df

def get_table_data_with_condition(conn, table_name, condition, params):
    """
    Read data based on condition.
    :param condition: Query condition, e.g., "[YearMonth] = ?"
    :param params: Values for the condition parameters
    """
    query = f"SELECT * FROM {table_name} WHERE {condition}"
    df = pd.read_sql(query, conn, params=params)
    return df

def delete_table(conn, table_name):
    """
    Delete the specified table.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP TABLE {table_name}")
        conn.commit()
        print(f"Table {table_name} deleted")
    except pyodbc.Error as e:
        print(f"Table {table_name} deletion failed: {e}")
    finally:
        cursor.close()

def delete_rows_with_condition(conn, table_name, condition, params):
    """
    Delete rows based on condition.
    :param condition: Query condition, e.g., "[YearMonth] = ?"
    :param params: Values for the condition parameters
    """
    cursor = conn.cursor()
    delete_query = f"DELETE FROM {table_name} WHERE {condition}"
    cursor.execute(delete_query, params)
    conn.commit()
    cursor.close()

def insert_dataframe_to_db(df, conn, entity, table_name):
    """
    Save DataFrame data to Access database.
    """
    df = replace_null_values(df)
    cursor = conn.cursor()

    # Create table (if not exists)
    create_table_from_entity(conn, table_name, entity)

    # Insert data
    for _, row in df.iterrows():
        placeholders = ", ".join(["?" for _ in row])
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        try:
            cursor.execute(insert_query, tuple(row))
        except pyodbc.Error as e:
            # print(f"Column[Submitdate]={row['Submitdate']} type={type(row['Submitdate'])}")
            # print(f"Column[Submitdate_Yearmonth]={row['Submitdate_Yearmonth']} type={type(row['Submitdate_Yearmonth'])}")
            # print(f"Column[Last_Resolved_Date]={row['Last_Resolved_Date']} type={type(row['Last_Resolved_Date'])}")
            # print(f"Column[Last_Resolved_Yearmonth]={row['Last_Resolved_Yearmonth']} type={type(row['Last_Resolved_Yearmonth'])}")
            raise Exception(f"Insertion row {row.to_json()} failed: {e}")

    conn.commit()
    cursor.close()

def replace_null_values(df):
    df = df.replace({np.nan: None})
    df = df.replace({pd.NaT: None})
    df = df.replace({"nan": None})
    return df
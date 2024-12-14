import numpy as np
import db_access_helper as db
import pandas as pd
from db_access_helper import *

def load_excel_by_month(file_path, year_month, sheet):
    """
    Load data based on the specified Excel file path and year-month.
    :param file_path: Excel file path
    :param year_month: Year-month (format: yyyy-MM)
    :return DataFrame containing original columns
    """
    print(f"Loading data from {file_path} for {year_month}...")
    df = pd.read_excel(file_path, sheet_name=sheet)
    df = df.replace({None: np.nan})
    df['YearMonth'] = year_month  # Add year-month column identifier
    return df


def load_tickets_from_excel(database_path, year_month, excel_file, sheet, drop_target_table=False):
    """
    Load tickets from the specified Excel file to the database.
    :param database_path: Database path
    :param year_month: Year-month (format: yyyy-MM)
    :param excel_file: Excel file path
    :param sheet: Sheet name
    :param drop_target_table: Whether to drop the target table before inserting data
    :return: List of TicketRawEntity instances
    """

    if not os.path.exists(excel_file):
        raise FileNotFoundError(f"Excel file not found: {excel_file}")
    if sheet not in pd.ExcelFile(excel_file).sheet_names:
        raise ValueError(f"Sheet {sheet} not found in Excel file {excel_file}")

    table_name = f"data_{year_month.replace("-", "_")}"  # Name table by data_year_month format

    # Connect to the database
    print("Checking or creating database...")
    conn = db.connect_to_database(database_path)

    # Drop the target table if specified
    if drop_target_table:
        db.delete_table(conn, table_name)


    # Load data from the specified Excel file
    print("Loading data from specified Excel file...")
    raw_data = load_excel_by_month(excel_file, year_month, sheet)

    # Map each row to an instance of TicketRawEntity class
    tickets = {TicketRawEntity.get_from_excel(row) for _, row in raw_data.iterrows()}

    # Convert tickets to dataframe
    df = pd.DataFrame([t.__dict__ for t in tickets])

    # Save to database
    conn = db.connect_to_database(database_path)
    db.insert_dataframe_to_db(df, conn, table_name, TicketRawEntity())
    conn.close()

    return tickets

def aggregate_tickets_from_source(source_database_path, target_database_path, target_table_name, drop_target_table=False):
    """
    Aggregate tickets from the source database to the target database.
    :param source_database_path: Source database path
    :param target_database_path: Target database path
    :param target_table_name: Target table name
    :param drop_target_table: Whether to drop the target table before inserting data
    """
    source_conn = db.connect_to_database(source_database_path)
    target_conn = db.connect_to_database(target_database_path)

    if drop_target_table:
        db.delete_table(target_conn, target_table_name)

    # Read all tables from the source database
    tables = db.get_tables_and_size(source_conn)
    print(f"Tables in source database: {tables}")

    # Read table data from the source database and save to the target database
    for table_name in tables:
        print(f"Reading data from {table_name}...")
        df = db.get_table_data(source_conn, table_name)
        db.insert_dataframe_to_db(df, target_conn, target_table_name, TicketRawEntity())

    print(f"Data aggregated to {target_table_name} in {target_database_path}")

    source_conn.close()
    target_conn.close()


def preprocess_data(data):
    """
    Preprocess data, such as merging columns or removing null values.
    """
    # Assume columns A to BE are inputs, and keep column BF as the target
    input_columns = data.columns[:-1]  # A to BE
    output_column = "BF"  # BF column

    # Merge input columns into a single string column
    data['input'] = data[input_columns].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
    data = data[['input', output_column]].dropna()  # Remove rows without summary
    return data
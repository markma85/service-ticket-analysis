from data_loader import load_tickets_from_excel
import db_access_helper as db
import data_cleansier as dc
import os

from src.entity.ticket_cleaned_entity import TicketCleanedEntity


def load_file(database_path):

    # year_month = "2024-1"  # Example year-month
    # excel_file = f"../static/raw_data/2023-12/Polaris Ticket Analysis_Dec(已自动还原).xlsx"
    # excel_file = f"../static/raw_data/2024-1/Weekly ticket analysis Jan.xlsx"
    # excel_file = f"../static/raw_data/2024-2/Feb ticekt.xlsx"
    # excel_file = f"../static/raw_data/2024-3/Mar ticket.xlsx"
    # excel_file = f"../static/raw_data/2024-3/Heming-EMEA-Polaris Ticket Analysis_Mar.xlsx" # TODO: Format 2
    # excel_file = f"../static/raw_data/2024-3/Mar ticket.xlsx"
    # excel_file = f"../static/raw_data/2024-4/Polaris Ticket Analysis_Apr_Wenxi (1).xlsx" # TODO: Format 2
    # excel_file = f"../static/raw_data/2024-4/April ticket.xlsx"
    # excel_file = f"../static/raw_data/2024-5/May ticket.xlsx"
    # excel_file = f"../static/raw_data/2024-6/June ticket.xlsx"
    # excel_file = f"../static/raw_data/2024-7/Copy of Polaris Ticket Analysis_July(已自动还原) (version 1).xlsb.xlsx"
    # excel_file = f"../static/raw_data/2024-8/Polaris Ticket Analysis_Aug 的副本.xlsx"
    # excel_file = f"../static/raw_data/2024-9/Polaris Ticket Analysis_Sep 的副本.xlsx"
    # excel_file = f"../static/raw_data/2024-10/Oct.xlsx"
    excel_file = f""
    sheet = "Sheet1"

    year_month = os.path.basename(os.path.dirname(excel_file))

    tickets = load_tickets_from_excel(database_path, year_month, excel_file, sheet, False)



def delete_table(database_path, table_name):
    db.delete_table(
        db.connect_to_database(database_path),
        table_name
    )

def main():
    # Configure path
    raw_database_path = "../static/tickets.accdb"
    aggregate_database_path = "../static/aggregate.accdb"
    # load_file(raw_database_path)
    delete_table(aggregate_database_path, "data_cleaned")

    # aggregate_tickets_from_source(raw_database_path, aggregate_database_path, "data_raw", True)

    df = db.get_table_data(
        db.connect_to_database(aggregate_database_path),
        "data_raw"
    )
    df = dc.data_imputation(df)
    df = dc.data_transformation(df)
    db.insert_dataframe_to_db(df, db.connect_to_database(aggregate_database_path), TicketCleanedEntity(),"data_cleaned")


if __name__ == "__main__":
    main()
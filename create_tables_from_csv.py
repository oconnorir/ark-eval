import sys
from psycopg2 import sql

# DATABASE CONNECTION
from db_connection import start_db_connection
# UTILITY FUNCTIONS
from utilities import parse_csv
from utilities import csv_to_string


def create_tables(metadata):
    # CONNECT TO DB
    connection = start_db_connection()
    cursor = connection.cursor()

    # ASSUMING THAT TABLES WILL BE CREATED FROM SCRATCH...
    # ENSURE THAT TABLES ARE CREATED CLEANLY BY DROPPING
    # ALL CURRENT TABLES FROM DATABASE
    sql.SQL("DROP SCHEMA public CASCADE;")
    sql.SQL("CREATE SCHEMA public;")
    try:
        # GROUP METADATA BY TABLE NAME
        table_definitions = {}
        for row in metadata:
            table_name = row["TABLE_NAME"]
            column_definition = f"{row['COLUMN_NAME']} {row['DATA_TYPE']}"
            if table_name not in table_definitions:
                table_definitions[table_name] = []
            table_definitions[table_name].append(column_definition)

        # CREATE TABLES
        for table_name, columns in table_definitions.items():
            create_table_query = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {table} ({columns});"
            ).format(
                table=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(map(sql.SQL, columns))
            )
            cursor.execute(create_table_query)
            print(f"Table '{table_name}' created successfully.")

        connection.commit()
    except Exception as e:
        print(f"There was an error creating tables: {e}")
    finally:
        cursor.close()
        connection.close()


# ADD PATH TO FILE WHEN CALLING THIS
# FUNCTION FROM THE COMMAND LINE
# EX: python3 create_tables_from_csv.py db_files/schema_setup/INFORMATION_SCHEMA.csv
file_path_argument = sys.argv[1]

stringed_csv = csv_to_string(file_path_argument)
parsed_metadata = parse_csv(stringed_csv)

# CREATE TABLES BASED ON THE PARSED METADATA
create_tables(parsed_metadata)

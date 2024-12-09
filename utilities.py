import csv

"""
    These are utility functions for handling and parsing csv files
"""


def csv_to_string(csv_local_file_path):
    """
    Converts the csv at the path given to a string representation
    :param csv_local_file_path: Path to local csv file that defines the schema for database tables
    :return: A string representation of the given csv
    """
    with open(csv_local_file_path, 'r') as db_schema:
        csv_reader = csv.reader(db_schema)
        csv_string = ""
        for row in csv_reader:
            csv_string += ','.join(row) + '\n'
        return csv_string


def parse_csv(data):
    """
    Parses up the string representation of the database schema then restructures it for integration
    into a query construction function
    :param data: A string representation of the schema used to create the tables for the database
    :return: A list of dictionaries each of which holds the data necessary for the construction or
    the 'create table' queries
    """
    lines = data.strip().split("\n")
    headers = [header.strip() for header in lines[0].split(",")]
    rows = [line.split(" ") for line in lines[1:]]
    parsed = []
    for row in rows:
        record = {headers[i]: row[i].strip().strip('"').strip(",").lower() for i in range(len(headers))}
        parsed.append(record)
    return parsed
